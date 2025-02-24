import ast

source_code = "3 + 4"
tree = ast.parse(source_code, mode='eval')

print(ast.dump(tree))
