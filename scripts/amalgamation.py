import os
import re

target = os.path.join('duckdb', 'duckdb_stable.hpp')
written_files = {}
include_paths = 'duckdb'

def open_utf8(fpath: str, flags: str) -> object:
    return open(fpath, flags, encoding="utf8")

def get_includes(fpath, text):
    # find all the includes referred to in the directory
    regex_include_statements = re.findall("(^[\t ]*[#][\t ]*include[\t ]+[\"]([^\"]+)[\"])", text, flags=re.MULTILINE)
    include_statements = []
    include_files = []
    # figure out where they are located
    for x in regex_include_statements:
        included_file = x[1]
        include_statements.append(x[0])
        if not included_file.startswith('duckdb/stable'):
        	continue
        include_files.append(included_file)
    return (include_statements, include_files)

def cleanup_file(text):
    # remove all "#pragma once" notifications
    text = re.sub('#pragma once', '', text)
    text = re.sub('\n\n\n+', '\n\n', text)
    text = re.sub(r'[/]+[=-]+[/]+.*[/]+[=-]+[/]+', '', text, flags=re.DOTALL)
    return text

def amalgamate_file(current_file):
    global written_files
    if current_file in written_files:
    	return ''
    print(current_file)
    written_files[current_file] = True
    with open_utf8(current_file, 'r') as f:
        text = f.read()
    # get all include files
    (statements, includes) = get_includes(current_file, text)
    # now write all the dependencies of this header first
    for i in range(len(includes)):
        include_text = amalgamate_file(includes[i])
        text = text.replace(statements[i], include_text)
    return cleanup_file(text)


def amalgamate_dir(path):
	result = ''
	for fname in os.listdir(path):
		full_path = os.path.join(path, fname)
		if fname == 'duckdb_stable.hpp':
			continue
		if os.path.isdir(full_path):
			result += amalgamate_dir(full_path)
		elif os.path.isfile(full_path):
			result += amalgamate_file(full_path)
	return result

content = '#pragma once\n\n'
content += amalgamate_dir('duckdb')

with open(target, 'w+') as f:
	f.write(content)