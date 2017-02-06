#!/usr/bin/env python3
# coding=utf-8

def build_singlefile_slaver(outfile_name="slaver_singlefile.py"):
    import os
    import shutil
    base_path = os.path.dirname(os.path.abspath(__file__))
    output_fullpath = os.path.join(base_path, outfile_name)

    if os.path.exists(output_fullpath):
        ch = input("target file: " + output_fullpath + " already exist, overwrite it?\n(y/N)")
        if ch not in ("y", "Y", "yes"):
            print("user abort")
            return None
        else:
            os.remove(output_fullpath)

    shutil.copy(
        os.path.join(base_path, "common_func.py"),
        os.path.join(base_path, outfile_name, )
    )
    with open("slaver.py", "r", encoding='utf-8') as fr:
        slaver = fr.read()

    slaver = slaver.replace("from __future__ import", "# from __future__ import", 1)
    slaver = slaver.replace("from common_func import", "# from common_func import", 1)

    with open(outfile_name, "a", encoding="utf-8") as fw:
        fw.write(slaver)

    print("generate complete!\noutput file:", output_fullpath)


if __name__ == '__main__':
    build_singlefile_slaver()
