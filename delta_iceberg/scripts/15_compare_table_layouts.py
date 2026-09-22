import os


def show_directory(path):
    print(f"\n=== {path} ===")

    for root, dirs, files in os.walk(path):
        level = root.replace(path, "").count(os.sep)

        if level > 2:
            continue

        indent = "    " * level
        print(f"{indent}{os.path.basename(root)}/")

        for file in files[:10]:
            print(f"{indent}    {file}")


show_directory("warehouse/delta/flights")
show_directory("warehouse/iceberg/bts/flights")