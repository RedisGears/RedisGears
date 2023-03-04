#!/usr/bin/env python3

import zipfile
import os
import shutil
import json
import tempfile

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))

directories = ["bin/artifacts/release/", "bin/artifacts/snapshot/"]

for dir in directories:
    dir = os.path.join(ROOT, dir)
    for f in [a for a in os.listdir(dir)]:
        name, ext = os.path.splitext(f)
        if ext == '.zip' and 'withdeps' not in name:
            with tempfile.TemporaryDirectory() as tmpdirname:
                print(f"Appending deps for {name}, working dir {tmpdirname}")
                os.chdir(os.path.abspath(tmpdirname))
                os.makedirs('deps')
                with zipfile.ZipFile(os.path.join(dir, f), 'r') as zip_ref:
                    zip_ref.extractall("./")
                    data = json.load(open('./module.json'))
                    for dep in data['dependencies'].values():
                        url = dep['url']
                        deps_file_name = url.split('/')[-1]
                        shutil.copy(os.path.join(dir, deps_file_name), os.path.join(os.getcwd(), 'deps', deps_file_name))
                shutil.make_archive(os.path.join(dir, f'{name}-withdeps'), 'zip', './')