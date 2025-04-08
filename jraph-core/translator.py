import re
import sys
import textwrap
from pathlib import Path

func_reg = []


def map_arg(arg):
    if "*" not in arg:
        return "usize"


def map_rust_arg(arg):
    # second return for is_primitive
    if "Vec<Complex32>" in arg:
        return "VecC32", False
    elif "DVector<Complex32>" in arg:
        return "DVectorC32", False
    elif "DMatrix<Complex32>" in arg:
        return "DMatrixC32", False
    elif arg == "String":
        return "String", False
    else:
        return arg.capitalize(), True


def retrieve_type(arg, arg_name):
    match arg:
        case "VecC32":
            return f"{arg_name}.as_ref()"
        case "DVectorC32":
            return arg_name
        case "DMatrixC32":
            return arg_name
        case _:
            return f"{arg_name}.clone()"

def retrieve_refmut(arg):
    ref = "&" if "&" in arg else ""
    mut = "mut" if "mut" in arg else ""
    return ref, mut


def create_arg_retrieve(index, arg_name, arg_type):

    ref_arg, mut_arg = retrieve_refmut(arg_type)
    clean_type = arg_type.replace("&", "").replace("mut", "")
    print(f"arg_name: {arg_name}, arg_type: {arg_type}, clean_type: {clean_type}")

    arg_proc, is_prim = map_rust_arg(arg_type)
    retr_type = retrieve_type(arg_proc, arg_name)
    ref = "&" if not is_prim else ""

    if arg_type[0:3] == "Vec":
        arg_ret = (
            f"\tlet mut {arg_name}: {arg_type} = Vec::new();\n"
            f"\tfor i in 0..args.len() {{\n"
            f"\t\t let x = match {ref}args[i] {{\n"
            f"\t\t\tCmTypes::{arg_proc}(x) => x,\n"
            f'\t\t\t_ => panic!("Invalid argument type"),\n'
            f"\t\t }};\n"
            f"\t\t {arg_name}.push(x);\n"
            f"\t}};\n"
        )
    else:
        arg_ret = (
            f"\tlet {arg_name}: {arg_type} = match {ref}args[{index}] {{\n"
            f"\t\tCmTypes::{arg_proc}({arg_name}) => {retr_type},\n"
            f'\t\t_ => panic!("Invalid argument type"),\n'
            f"\t}};"
        )
    return arg_ret

def create_arg_return(return_type, fn_name, arg_names):
    arc_ret = f"Arc::new({fn_name}({arg_names}))"
    norm_ret = f"{fn_name}({arg_names})"
    return_type, _ = map_rust_arg(return_type)

    if "C32" in return_type:
        func_call = f"\tCmTypes::{return_type}({arc_ret})\n}}\n"
    else:
        func_call = f"\tCmTypes::{return_type}({norm_ret})\n}}\n"
    
    return func_call


def call_arc(return_type, fn_name, arg_names):
    return_type, _ = map_rust_arg(return_type)
    func_call = f"\tCmTypes::{return_type}(Arc::new({fn_name}({arg_names})))\n}}\n"
    return func_call


def get_func_call(return_type, fn_name, arg_names):
    return_type, _ = map_rust_arg(return_type)
    func_call = f"\tCmTypes::{return_type}({fn_name}({arg_names}))\n}}\n"


def generate_wrappers(functions, mode="rust"):
    wrappers = []
    externC = []
    for fn_name, args_signature, return_type in functions:
        wrapper, extern = generate_wrapper(fn_name, args_signature, return_type, mode)
        wrappers.append(wrapper)
        externC.extend(extern)

    return "\n".join(wrappers), "".join(externC)


def extract_function_signatures(content, mode="rust"):
    if mode == "rust":
        pattern = re.compile(r"pub fn (\w+)\s*\(([^)]*)\)\s*(->\s*([^ \{]+))?")
    elif mode == "cpp":
        pattern = re.compile(r"(\w+)\s+(\w+)\s*\(([^)]*)\)\s*;")

    matches = pattern.findall(content)
    function_signatures = []
    for match in matches:
        if mode == "rust":
            fn_name = match[0]
            args = match[1]
            return_type = match[3] if match[2] else None
        elif mode == "cpp":
            fn_name = match[1]
            args = match[2]
            return_type = match[0]
        function_signatures.append((fn_name, args, return_type))
    return function_signatures


def generate_wrapper(fn_name, args_signature, return_type, mode="rust"):
    args = (
        [arg.strip() for arg in args_signature.split(",") if arg.strip()]
        if args_signature
        else []
    )
    arg_names = []
    arguments = []

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

    arg_names_str = ", ".join(arg_names)
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
    signature = f"pub fn {fn_name}_wrap({arg_sign}) -> CmTypes {{\n"
    if arguments == []:
        func_call = f"\t{fn_name}({arg_names_str});\n"
        ret_cm = f"\tCmTypes::None()\n}}\n"
        complete = signature + func_call + ret_cm
    else:
        body = f"{match_arms_str}\n\n"
        if mode == "cpp":
            # unsafe call for C++ functions
            func_call = f"\tCmTypes::{return_type_str.capitalize()}(unsafe{{{fn_name}({arg_names_str})}})\n}}\n"
            # func_call = f'\tunsafe{{{fn_name}({arg_names_str})}}\n}}\n'
        else:
            func_call = create_arg_return(return_type_str, fn_name, arg_names_str)
        complete = signature + body + func_call

    has_args = True if len(arguments) > 0 else False
    func_reg.append((fn_name, f"{fn_name}_wrap", has_args))

    return complete, externC


def handle_rust(content, output_file):
    mode = "rust"
    function_signatures = extract_function_signatures(content, mode)
    wrapper_code, _ = generate_wrappers(function_signatures, mode)

    with output_file.open("w") as f:
        # include the original function files
        f.write(f"use crate::{input_file.stem}::*;\n\n")
        # include shared::CmTypes
        f.write("use crate::cmtypes::CmTypes;\n")
        # include Complex32
        f.write("use num_complex::Complex32;\n")
        # include Arc
        f.write("use std::sync::Arc;\n\n")
        # include nalgebra
        f.write("use nalgebra::*;\n\n")
        f.write(wrapper_code)


def handle_cpp(content, file_name, output_file):
    mode = "cpp"
    function_signatures = extract_function_signatures(content, mode)
    wrapper_code, externC = generate_wrappers(function_signatures, mode)

    # add a \t to every line in wrapper code
    # as extern C will be added later
    externC = textwrap.indent(externC, "\t")

    with output_file.open("w") as f:
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


def create_func_registry(wrapper_file, registry_file):
    with registry_file.open("w") as f:
        # include generated wrappers
        f.write(f"use crate::{wrapper_file.stem}::*;\n")
        # include shared::CmTypes
        f.write("use crate::cmtypes::*;\n\n")

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
        f.write('\t\t_ => panic!("Function not found"),\n')
        f.write("\t}\n")
        f.write("}\n")


def create_emtpy_registry(registry_file):
    with registry_file.open("w") as f:
        # include shared::CmTypes
        f.write("use crate::cmtypes::*;\n\n")
        # function signature
        f.write("pub fn get_func(_func_name: &str) -> Option<CmPtr> {\n")
        f.write("\tNone\n")
        f.write("}\n")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: translator.py <function_file> <wrapper_file> <registry_file> <python>"
        )
        exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])
    registry_file = Path(sys.argv[3])

    python_version = Path(sys.argv[4])
    if str(python_version) == "True":
        print("Creating empty registry")
        create_emtpy_registry(registry_file)
        exit(0)

    with input_file.open("r") as f:
        content = f.read()

    file_name = input_file.stem
    file_extension = input_file.suffix

    if file_extension == ".rs":
        handle_rust(content, output_file)
    elif file_extension == ".h":
        handle_cpp(content, file_name, output_file)

    create_func_registry(output_file, registry_file)
