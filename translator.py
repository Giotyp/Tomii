import re
import sys
import textwrap
from pathlib import Path

def map_arg(arg):
    if '*' not in arg:
        return 'usize'


def generate_wrappers(functions, mode='rust'):
    wrappers = []
    externC = []
    for fn_name, args_signature, return_type in functions:
        wrapper, extern = generate_wrapper(fn_name, args_signature, return_type, mode)
        wrappers.append(wrapper)
        externC.extend(extern)

    return '\n'.join(wrappers), ''.join(externC)


def extract_function_signatures(content, mode='rust'):
    if mode == 'rust':
        pattern = re.compile(r"pub fn (\w+)\s*\(([^)]*)\)\s*(->\s*([^ \{]+))?") 
    elif mode == 'cpp': 
        pattern = re.compile(r"(\w+)\s+(\w+)\s*\(([^)]*)\)\s*;")

    matches = pattern.findall(content)
    function_signatures = []
    for match in matches:
        if mode == 'rust':
            fn_name = match[0]
            args = match[1]
            return_type = match[3] if match[2] else None
        elif mode == 'cpp':
            fn_name = match[1]
            args = match[2]
            return_type = match[0]
        function_signatures.append((fn_name, args, return_type))
    return function_signatures


def generate_wrapper(fn_name, args_signature, return_type, mode='rust'):
    args = [arg.strip() for arg in args_signature.split(
        ',') if arg.strip()] if args_signature else []
    arg_names = []
    cmtypes = []

    arg_list = []

    for index, arg in enumerate(args):

        if mode == 'rust':
            arg_details = arg.split(':')
            arg_name = arg_details[0].strip()
            arg_type = arg_details[1].strip()
        elif mode == 'cpp':
            arg_details = arg.split()
            arg_name = arg_details[1].strip()
            arg_type = map_arg(arg_details[0].strip())

            arg_list.append(f'{arg_name}: {arg_type}')

        arg_names.append(arg_name)

        clean_type = arg_type.replace('&', "").replace(
            "mut", "").replace(" ", "")
        
        ref = "&" if clean_type == "String" else ""

        cmtypes.append(
            f'\tlet {arg_name} = match {ref}args[{index}] {{\n'
            f'\t\tCmTypes::{clean_type.capitalize()}({arg_name}) => {arg_name}.clone(),\n'
            f'\t\t_ => panic!("Invalid argument type"),\n'
            f'\t}};')

    arg_names_str = ', '.join(arg_names)
    match_arms_str = '\n\n'.join(cmtypes)

    if mode == 'cpp':
        return_type = map_arg(return_type)
    return_type_str = return_type if return_type else '()'


    if len(arg_list) > 0:
        externC = f'fn {fn_name}(\n'
        for arg_tp in arg_list:
            externC += f'\t{arg_tp},\n'
        externC += f') -> {return_type_str};\n'
    else:
        externC = f'fn {fn_name}() -> {return_type_str};\n'

    if cmtypes == []:
        return(
        f'pub fn {fn_name}_wrap() -> {return_type_str} {{\n'
        f'\t{fn_name}({arg_names_str})\n'
        f'}}\n', externC)
    else:
        if mode == 'cpp':
            return(
            f'pub fn {fn_name}_wrap(args: Vec<CmTypes>) -> {return_type_str} {{\n'
            f'{match_arms_str}\n\n'
            f'\tunsafe{{{fn_name}({arg_names_str})}}\n'
            f'}}\n', externC)
        else:
            return(
            f'pub fn {fn_name}_wrap(args: Vec<CmTypes>) -> {return_type_str} {{\n'
            f'{match_arms_str}\n\n'
            f'\t{fn_name}({arg_names_str})\n'
            f'}}\n', externC)


def handle_rust(content, output_file):
    mode = 'rust'
    function_signatures = extract_function_signatures(content, mode)
    wrapper_code, _ = generate_wrappers(function_signatures, mode)

    with output_file.open('w') as f:
        # include shared::CmTypes
        f.write("use shared::CmTypes;\n")
        # include the original function files
        f.write(f"use crate::{input_file.stem}::*;\n\n")
        f.write(wrapper_code)

def handle_cpp(content, file_name, output_file):
    mode = 'cpp'
    function_signatures = extract_function_signatures(content, mode)
    wrapper_code, externC = generate_wrappers(function_signatures, mode)

    # add a \t to every line in wrapper code
    # as extern C will be added later
    externC = textwrap.indent(externC, '\t')  

    with output_file.open('w') as f:
        # include shared::CmTypes
        f.write("use shared::CmTypes;\n\n")
        # link with .so file
        f.write(f"#[link(name = \"{file_name}\")]\n")
        # extern C clause
        f.write("extern \"C\" {\n")
        f.write(externC)
        f.write("}\n")
        f.write("\n")
        f.write(wrapper_code)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: translator.py <input_file> <output_file>")
        exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])

    with input_file.open('r') as f:
        content = f.read()

    file_name = input_file.stem
    file_extension = input_file.suffix

    if file_extension == '.rs':
        handle_rust(content, output_file)
    elif file_extension == '.h':
        handle_cpp(content, file_name, output_file)
