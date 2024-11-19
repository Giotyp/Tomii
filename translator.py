import re
import sys
import textwrap
from pathlib import Path


def extract_function_signatures(content):
    pattern = re.compile(r"pub fn (\w+)\s*\(([^)]*)\)\s*(->\s*([^ \{]+))?")
    matches = pattern.findall(content)
    function_signatures = []
    for match in matches:
        fn_name = match[0]
        args = match[1]
        return_type = match[3] if match[2] else None
        function_signatures.append((fn_name, args, return_type))
    return function_signatures


def generate_wrappers(functions):
    wrappers = []
    for fn_name, args_signature, return_type in functions:
        wrapper = generate_wrapper(fn_name, args_signature, return_type)
        wrappers.append(wrapper)
    return '\n'.join(wrappers)


def generate_wrapper(fn_name, args_signature, return_type):
    args = [arg.strip() for arg in args_signature.split(
        ',') if arg.strip()] if args_signature else []
    arg_names = []
    match_arms = []

    print(args)

    for index, arg in enumerate(args):
        arg_details = arg.split(':')
        arg_name = arg_details[0].strip()
        arg_type = arg_details[1].strip()

        arg_names.append(arg_name)

        clean_type = arg_type.replace('&', "").replace(
            "mut", "").replace(" ", "")
        
        ref = "&" if clean_type == "String" else ""

        match_arms.append(
            f'\tlet {arg_name} = match {ref}args[{index}] {{\n'
            f'\t\tCmTypes::{clean_type.capitalize()}({arg_name}) => {arg_name}.clone(),\n'
            f'\t\t_ => panic!("Invalid argument type"),\n'
            f'\t}};')

    arg_names_str = ', '.join(arg_names)
    match_arms_str = '\n\n'.join(match_arms)
    return_type_str = return_type if return_type else '()'

    if match_arms == []:
        return (
            f'pub fn {fn_name}_wrap() -> {return_type_str} {{\n'
            f'\t{fn_name}({arg_names_str})\n'
            f'}}\n')
    else:
        return (
            f'pub fn {fn_name}_wrap(args: Vec<CmTypes>) -> {return_type_str} {{\n'
            f'{match_arms_str}\n\n'
            f'\t{fn_name}({arg_names_str})\n'
            f'}}\n')


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: translator.py <input_file> <output_file>")
        exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])

    with input_file.open('r') as f:
        content = f.read()

    function_signatures = extract_function_signatures(content)
    wrapper_code = generate_wrappers(function_signatures)

    with output_file.open('w') as f:
        # include shared::CmTypes
        f.write("use shared::CmTypes;\n")
        # include the original function files
        f.write(f"use crate::{input_file.stem}::*;\n\n")
        f.write(wrapper_code)
