import zipfile
import os
import shutil
import json

directories = ["artifacts/release/", "artifacts/snapshot/"]

BASE_DIR = dir_path = os.path.dirname(os.path.realpath(__file__))

for dir in directories:
    dir = os.path.join(BASE_DIR, dir)
    os.chdir(dir)
    for f in os.listdir('./'):
        name, ext = os.path.splitext(f)
        if ext == '.zip' and 'withdeps' not in name:
            print('Appending deps for %s' % name)
            os.mkdir('./tmp/')
            os.chdir('./tmp/')
            os.mkdir('./deps/')
            with zipfile.ZipFile(os.path.join('../', f), 'r') as zip_ref:
                zip_ref.extractall("./")
                f = open('./module.json')
                data = json.load(f)
                for dep in data['dependencies'].values():
                    url = dep['url']
                    deps_file_name = url.split('/')[-1]
                    shutil.copy(os.path.join('../', deps_file_name), os.path.join('./deps/', deps_file_name))
            shutil.make_archive(os.path.join('../', '%s-withdeps' % name), 'zip', './')
            os.chdir('../')
            shutil.rmtree('./tmp/')
