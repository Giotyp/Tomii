import re
import sys
import textwrap
from pathlib import Path

func_reg = []

defined_types = [
    "bool",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "isize",
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "usize",
    "f32",
    "f64",
    "String",
    "C32",
    "Vec<CmTypes>",
]


def map_arg(arg):
    if "*" not in arg:
        return "usize"


def map_cmtype_entry(arg):
    """
    Map a a Rust type to a CmTypes enum variant.
    This function also determines if the type is a primitive type.
    Parameters
    ----------
    arg : str
        The Rust type to map.
    Returns
    -------
    tuple
        A tuple containing the mapped CmTypes variant and a boolean
        indicating if the type is a primitive type.
    """
    # second return for is_primitive
    if arg == "()":
        return "None", False
    elif arg in defined_types:
        return arg.capitalize(), True
    else:
        return "from_any", False


def get_arg_type(arg):
    """
    Extract the argument type from a vector definition.
    E.g. "Vec<usize>" -> "usize"
    """
    start_pos = arg.find("<")
    end_pos = arg.rfind(">")
    if start_pos != -1 and end_pos != -1:
        return arg[start_pos + 1 : end_pos]


def retrieve_type(arg, arg_name):
    match arg:
        case "VecC32":
            return f"{arg_name}.as_ref()"
        case "DVectorC32":
            return arg_name
        case "DMatrixC32":
            return arg_name
        case "Fft":
            return f"Arc::clone({arg_name})"
        case _:
            return f"{arg_name}.clone()"


def retrieve_refmut(arg):
    ref = "&" if "&" in arg else ""
    mut = "mut" if "mut" in arg else ""
    return ref, mut


def create_arg_retrieve(index, arg_name, arg_type):
    """
    Generate the Rust code snippet that pulls an argument out of a
    CmTypes vector  and casts it to the desired Rust type.

    Parameters
    ----------
    index : int
        Position in the `args` array to retrieve (ignored when `Vec`).
    arg_name : str
        Identifier to use for the local variable in the generated code.
    arg_type : str
        Rust type string, e.g. "&Foo", "&mut Foo", "Vec<&Bar>", "i32", etc.

    Returns
    -------
    str
        A Rust code snippet that:
        - Declares a local variable with name `arg_name` and type `arg_type`.
        - Matches on `args[index]` (or iterates for `Vec`) to extract the
            underlying value, panicking on mismatch.
    """

    # Check if the argument is a reference or mutable reference
    # ref_arg, mut_arg = retrieve_refmut(arg_type)

    # Strip out Rust’s & and mut markers
    # clean_type = arg_type.replace("&", "").replace("mut", "")

    # match on enum variant

    #  strip arg_type of &mut and store them if exist in stem_var

    arg_strip = arg_type.replace("&", "").replace("mut", "").strip()

    arg_proc, is_prim = map_cmtype_entry(arg_strip)

    # bind the inner value (e.g. as_ref(), clone(), etc.)
    retr_type = retrieve_type(arg_proc, arg_name)
    ref = "&" if not is_prim else ""

    ref_arg = "&" if "&" in arg_type else ""
    mut = "mut" if "mut" in arg_type else ""
    ref_mut = f"{ref_arg}{mut} "

    obj_ref = ""
    if "Arc" in retr_type:
        arg_type = f"Arc<Mutex<{arg_type}>>"
        obj_ref = "ref "

    # For `Vec<...>` arguments, build up a mutable Vec by iterating all args.
    if arg_type[0:3] == "Vec":
        arg_raw_type = get_arg_type(arg_type)
        raw_arg, raw_prim = map_cmtype_entry(arg_raw_type)
        ref = "&" if not raw_prim else ""
        # For now assume primitive types

        if arg_type in defined_types:
            arg_ret = (
                f"\tlet mut {arg_name}: {arg_type} = Vec::new();\n"
                # Take VecCmt buffer
                f"\tlet raw_arg_buffer = match &{ref}args[0] {{\n"
                f"\t\tCmTypes::{arg_proc}({obj_ref}{arg_name}) => {retr_type},\n"
                f'\t\t_ => panic!("Invalid argument type"),\n'
                f"\t}};\n"
                # Collect arguments
                f"\tfor i in 0..raw_arg_buffer.len() {{\n"
                f"\t\t let x = match {ref}raw_arg_buffer[i] {{\n"
                f"\t\t\tCmTypes::{raw_arg}(x) => x,\n"
                f'\t\t\t_ => panic!("Invalid argument type"),\n'
                f"\t\t }};\n"
                f"\t\t {arg_name}.push(x);\n"
                f"\t}};\n"
            )
        else:
            arg_ret = (
                f"\tlet {arg_name} = match args[{index}].downcast_any() {{\n"
                f"\t\tSome(extracted) => extracted,\n"
                f'\t\tNone => panic!("Failed to downcast CmTypes::Any"),\n'
                f"\t}};\n"
            )
    else:
        if arg_type in defined_types:
            # Single-value case: pick the slot `index` and match against the enum.
            arg_ret = (
                f"\tlet {arg_name}: {arg_type} = match {ref}args[{index}] {{\n"
                f"\t\tCmTypes::{arg_proc}({obj_ref}{arg_name}) => {ref_mut}{retr_type},\n"
                f'\t\t_ => panic!("Invalid argument type"),\n'
                f"\t}};"
            )
        else:
            arg_ret = (
                f"\tlet {arg_name} = match args[{index}].downcast_any() {{\n"
                f"\t\tSome(extracted) => extracted,\n"
                f'\t\tNone => panic!("Failed to downcast CmTypes::Any"),\n'
                f"\t}};\n"
            )
    return arg_ret


def create_arg_return(
    return_type, fn_name, arg_names, struct_func=False, mut_lock=False
):
    """
    Generate the Rust code snippet that calls a function with
    arguments pulled out of a CmTypes vector and returns the
    result wrapped in a `CmTypes` enum.
    Parameters
    ----------
    return_type : str
        Rust type string, e.g. "&Foo", "&mut Foo", "Vec<&Bar>", "i32", etc.
    fn_name : str
        Name of the function to call.
    arg_names : str
        Comma-separated list of argument names to pass to the function.
    Returns
    -------
    str
        A Rust code snippet that:
        - Calls the function `fn_name` with arguments `arg_names`.
        - Wraps the result in a `CmTypes` enum variant.
        - Returns the wrapped result.
    """
    retcm_type, _ = map_cmtype_entry(return_type)

    # Check for struct function that needs to be called with . operator
    if struct_func:
        struct_name = fn_name.split("::")[0]
        func = fn_name.split("::")[1]
        if struct_name == retcm_type:
            # new() function that returns the struct
            # no need to lock object
            pass
        else:
            # lock struct_object

            if mut_lock:
                ins_mut = " mut "
            else:
                ins_mut = " "

            lock_obj = f'\tlet{ins_mut}lock_obj = struct_obj.lock().expect("Failed to lock object");\n'
            if retcm_type == "None":
                func_call = f"\tlock_obj.{func}({arg_names});\n"
                ret_none = f"\tCmTypes::None\n}}\n"
                return lock_obj + func_call + ret_none
            else:
                func_call = (
                    f"\tCmTypes::{retcm_type}(lock_obj.{func}({arg_names}))\n}}\n"
                )
                return lock_obj + func_call

    arc_ret = f"Arc::new({fn_name}({arg_names}))"
    arc_mutex_ret = f"Arc::new(Mutex::new({fn_name}({arg_names})))"
    norm_ret = f"{fn_name}({arg_names})"

    if "C32" in retcm_type:
        func_call = f"\tCmTypes::{retcm_type}({arc_ret})\n}}\n"
    elif struct_func:
        func_call = f"\tCmTypes::{retcm_type}({arc_mutex_ret})\n}}\n"
    else:
        if retcm_type == "VecCmt":
            arg_raw_type = get_arg_type(return_type)
            raw_arg, raw_prim = map_cmtype_entry(arg_raw_type)
            # create a new vector to gather the results
            func_call = (
                f"\tlet res = {norm_ret};\n"
                f"\tlet mut ret = Vec::new();\n"
                f"\tfor i in 0..res.len() {{\n"
                f"\t\tlet x = res[i];\n"
                f"\t\tret.push(CmTypes::{raw_arg}(x));\n"
                f"\t}};\n"
                f"\tCmTypes::{retcm_type}(ret)\n}}\n"
            )

        else:
            if retcm_type == "None":
                # first call function and then return None
                func_call = f"\t{norm_ret};\n" f"\tCmTypes::None\n}}\n"
            else:
                func_call = f"\tCmTypes::{retcm_type}({norm_ret})\n}}\n"

    return func_call


def call_arc(return_type, fn_name, arg_names):
    return_type, _ = map_cmtype_entry(return_type)
    func_call = f"\tCmTypes::{return_type}(Arc::new({fn_name}({arg_names})))\n}}\n"
    return func_call


def get_func_call(return_type, fn_name, arg_names):
    return_type, _ = map_cmtype_entry(return_type)
    func_call = f"\tCmTypes::{return_type}({fn_name}({arg_names}))\n}}\n"


def generate_wrappers(functions, structs, mode="rust"):
    wrappers = []
    externC = []
    for fn_name, args_signature, return_type in functions:
        wrapper, extern = generate_wrapper(fn_name, args_signature, return_type, mode)
        wrappers.append(wrapper)
        externC.extend(extern)

    # Handle structs
    for struct_name, str_functions in structs.items():
        for fn_name, args_signature, return_type in str_functions:
            wrapper, extern = generate_wrapper(
                f"{struct_name}::{fn_name}", args_signature, return_type, mode
            )
            wrappers.append(wrapper)
            externC.extend(extern)

    return "\n".join(wrappers), "".join(externC)


def find_funcs(pattern, content, mode="rust"):
    signatures = []
    matches = pattern.findall(content)
    for match in matches:
        if mode == "rust":
            fn_name = match[0]
            args = match[1]
            return_type = match[2]
        elif mode == "cpp":
            fn_name = match[1]
            args = match[2]
            return_type = match[0]
        signatures.append((fn_name, args, return_type))
    return signatures


def find_impl_blocks(content):
    # Find all 'impl' blocks with proper brace matching
    impl_starts = re.finditer(r"impl\s+(\w+)\s*{", content)
    impl_blocks = []

    for match in impl_starts:
        struct_name = match.group(1)
        start_pos = match.end()
        nesting = 1  # Start with nesting level 1
        end_pos = start_pos

        # Scan through content to find matching closing brace
        for i in range(start_pos, len(content)):
            if content[i] == "{":
                nesting += 1
            elif content[i] == "}":
                nesting -= 1
                if nesting == 0:  # closing brace
                    end_pos = i
                    break

        if nesting == 0:  # Only if closing brace found
            impl_body = content[start_pos:end_pos]
            impl_blocks.append((struct_name, impl_body))

    return impl_blocks


def find_structs(content, struct_pattern, struct_funcs_pat):
    # Search for structs and impl blocks
    structs = re.findall(struct_pattern, content)
    struct_impls = {}

    # Find implementation blocks
    impl_blocks = find_impl_blocks(content)

    for struct_name, impl_body in impl_blocks:
        if struct_name not in structs:
            continue
        struct_impls[struct_name] = []
        methods = re.findall(struct_funcs_pat, impl_body)
        for method in methods:
            fn_name = method[0]
            args = method[1]
            # 2nd argument is '-> return_type'
            return_type = method[3] if method[2] else None
            struct_impls[struct_name].append((fn_name, args, return_type))

    return struct_impls


def extract_function_signatures(content, mode="rust"):
    if mode == "rust":
        # free functions outside of structs
        # ^ in the begginig disregards indentation meaning that
        # stucture and impl blocks are not considered
        # get function name, arguments, arrow, and return type
        # pub fn function(arg1: type1, arg2: type2) -> return_type
        pattern = re.compile(
            r"(?m)^pub\s+fn\s+(\w+)\s*\(([^)]*)\)\s*(?:->\s*([^ {]+))?"
        )
        struct_pattern = re.compile(r"pub\s+struct\s+(\w+)")
        struct_funcs_pat = r"pub fn (\w+)\s*\(([^)]*)\)\s*(->\s*([^ \{]+))?"
    elif mode == "cpp":
        # get function name, arguments, and return type
        # return_type function(arg1: type1, arg2: type2)
        pattern = re.compile(r"(\w+)\s+(\w+)\s*\(([^)]*)\)\s*;")

    function_signatures = []

    function_signatures.extend(find_funcs(pattern, content, mode))
    struct_impls = find_structs(content, struct_pattern, struct_funcs_pat)

    return function_signatures, struct_impls


def generate_wrapper(fn_name, args_signature, return_type, mode="rust"):
    # Split the argument signature
    args = (
        [arg.strip() for arg in args_signature.split(",") if arg.strip()]
        if args_signature and "self" not in args_signature
        else []
    )

    struct_func = True if ":" in fn_name else False

    # Check if the function is a method of a struct
    # then the struct object is the first argument
    if "self" in args_signature:
        object_arg = f"struct_obj: {fn_name.split('::')[0]}"
        # place object_arg at the beginning of the args list
        args.insert(0, object_arg)

    # list to collect argument names
    arg_names = []
    arguments = []

    # used in C++ mode
    carg_list = []

    for index, arg in enumerate(args):

        if mode == "rust":
            arg_details = arg.split(":")
            arg_name = arg_details[0].strip()
            arg_type = arg_details[1].strip()
        elif mode == "cpp":
            arg_details = arg.split()
            arg_name = arg_details[1].strip()
            arg_type = map_arg(arg_details[0].strip())

            carg_list.append(f"{arg_name}: {arg_type}")

        arg_names.append(arg_name)

        arguments.append(create_arg_retrieve(index, arg_name, arg_type))

    # remove struct_obj from arg_names
    if "struct_obj" in arg_names:
        arg_names.remove("struct_obj")

    arg_names_str = ""
    downcast = False
    for arg in arguments:
        if "downcast_any" in arg:
            downcast = True
            break
    for arg_name in arg_names:
        if downcast:
            # append args with a *
            arg_names_str += f"*{arg_name}, "
        else:
            arg_names_str += f"{arg_name}, "

    # trim from the end
    arg_names_str = arg_names_str.rstrip(", ")
    match_arms_str = "\n\n".join(arguments)

    if mode == "cpp":
        return_type = map_arg(return_type)
    return_type_str = return_type if return_type else "()"

    if len(carg_list) > 0:
        externC = f"fn {fn_name}(\n"
        for arg_tp in carg_list:
            externC += f"\t{arg_tp},\n"
        externC += f") -> {return_type_str};\n"
    else:
        externC = f"fn {fn_name}() -> {return_type_str};\n"

    arg_sign = "args: Vec<CmTypes>" if len(arguments) > 0 else "_args: Vec<CmTypes>"
    fn_name_sig = fn_name.replace("::", "_")
    signature = f"pub fn {fn_name_sig.lower()}_wrap({arg_sign}) -> CmTypes {{\n"
    if arguments == []:
        func_call = f"\t{fn_name}({arg_names_str});\n"
        ret_cm = f"\tCmTypes::None\n}}\n"
        complete = signature + func_call + ret_cm
    else:
        body = f"{match_arms_str}\n\n"
        if mode == "cpp":
            # unsafe call for C++ functions
            func_call = f"\tCmTypes::{return_type_str.capitalize()}(unsafe{{{fn_name}({arg_names_str})}})\n}}\n"
            # func_call = f'\tunsafe{{{fn_name}({arg_names_str})}}\n}}\n'
        else:
            if return_type and "Self" in return_type:
                struct_name = fn_name.split("::")[0]
                return_type_str = struct_name

            if struct_func and "mut" in args_signature:
                mut_lock = True
            else:
                mut_lock = False

            func_call = create_arg_return(
                return_type_str, fn_name, arg_names_str, struct_func, mut_lock
            )
        complete = signature + body + func_call

    has_args = True if len(arguments) > 0 else False
    func_reg.append((fn_name, f"{fn_name_sig.lower()}_wrap", has_args))

    return complete, externC


def handle_rust(content, input_stem, wrapper_file):
    mode = "rust"
    function_signatures, struct_impls = extract_function_signatures(content, mode)
    wrapper_code, _ = generate_wrappers(function_signatures, struct_impls, mode)

    # get any use modules from the content to reuse in the wrapper code
    use_modules = re.findall(r"use\s+([a-zA-Z0-9_]+::[a-zA-Z0-9_]+);", content)

    with wrapper_file.open("w") as f:
        # insert warning attributes
        # include the original function file
        f.write(f"use crate::{input_stem}::*;\n")
        # include shared::CmTypes
        # include same modules
        for module in use_modules:
            f.write(f"use {module};\n")
        f.write("use crate::cmtypes::CmTypes;\n")
        f.write(wrapper_code)


def handle_cpp(content, file_name, wrapper_file):
    mode = "cpp"
    function_signatures = extract_function_signatures(content, mode)
    wrapper_code, externC = generate_wrappers(function_signatures, mode)

    # add a \t to every line in wrapper code
    # as extern C will be added later
    externC = textwrap.indent(externC, "\t")

    with wrapper_file.open("w") as f:
        # include shared::CmTypes
        f.write("use crate::cmtypes::CmTypes;\n\n")
        # link with .so file
        f.write(f'#[link(name = "{file_name}")]\n')
        # extern C clause
        f.write('extern "C" {\n')
        f.write(externC)
        f.write("}\n")
        f.write("\n")
        f.write(wrapper_code)


def handle_init(content, file_name, wrapper_file):
    mode = "init"
    function_signatures = extract_function_signatures(content, mode)


def create_func_registry(wrapper_file, registry_file):
    with registry_file.open("w") as f:
        # include generated wrappers
        f.write(f"use crate::{wrapper_file.stem}::*;\n")
        # include shared::CmTypes
        f.write("use synstream_types::*;\n\n")

        # registry to retrieve function pointers
        # function signature
        f.write("pub fn get_func(func_name: &str) -> Option<CmPtr> {\n")
        # match arms
        f.write("\tmatch func_name {\n")
        for fn_name, fn_wrap, has_args in func_reg:
            f.write(f'\t\t"{fn_name}" => {{\n')
            f.write(f"\t\t\tSome({fn_wrap})\n")
            f.write("\t\t},\n")
        # write last arm
        f.write(
            '\t\t_ => {\n\t\t\tprintln!("Function {} not found", func_name);\n\t\t\tpanic!("Panicking...");\n\t\t}\n'
        )
        f.write("\t}\n")
        f.write("}\n")


def create_emtpy_registry(registry_file):
    with registry_file.open("w") as f:
        # include shared::CmTypes
        f.write("use synstream_types::*;\n\n")
        # function signature
        f.write("pub fn get_func(_func_name: &str) -> Option<CmPtr> {\n")
        f.write("\tNone\n")
        f.write("}\n")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: transformer.py <function_file> <wrapper_file> <registry_file>")
        exit(1)

    function_file = Path(sys.argv[1])
    wrapper_file = Path(sys.argv[2])
    registry_file = Path(sys.argv[3])

    with function_file.open("r") as f:
        content = f.read()

    file_name = function_file.stem
    file_extension = function_file.suffix

    if file_extension == ".rs":
        handle_rust(content, function_file.stem, wrapper_file)
    elif file_extension == ".h":
        handle_cpp(content, file_name, wrapper_file)

    create_func_registry(wrapper_file, registry_file)
