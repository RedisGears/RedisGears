
import os
import sys
from .platform import OnPlatform, Platform

#----------------------------------------------------------------------------------------------

class Runner:
    def __init__(self, nop=False):
        self.nop = nop

    def run(self, cmd):
        if self.nop:
            print(cmd)
            return
        rc = os.system(cmd)
        if rc > 0:
            eprint("command failed: " + cmd)
            sys.exit(1)

    def has_command(self, cmd):
        return os.system("command -v " + cmd + " > /dev/null") == 0

#----------------------------------------------------------------------------------------------

class RepoRefresh(OnPlatform):
    def __init__(self, runner):
        OnPlatform.__init__(self)
        self.runner = runner

    def redhat_compat(self):
        pass
    
    def debian_compat(self):
        self.runner.run("apt-get update -y")

#----------------------------------------------------------------------------------------------

class Setup(OnPlatform):
    def __init__(self, nop=False):
        OnPlatform.__init__(self)
        self.runner = Runner(nop)
        self.stages = [0]
        self.platform = Platform()
        self.os = self.platform.os
        self.dist = self.platform.dist
        self.ver = self.platform.os_ver

    def setup(self):
        RepoRefresh(self.runner).invoke()
        self.invoke()

    def run(self, cmd):
        return self.runner.run(cmd)

    def has_command(self, cmd):
        return self.runner.has_command(cmd)

    #------------------------------------------------------------------------------------------

    def apt_install(self, packs, group=False):
        self.run("apt-get install -q -y " + packs)

    def yum_install(self, packs, group=False):
        if not group:
            self.run("yum install -q -y " + packs)
        else:
            self.run("yum groupinstall -y " + packs)

    def dnf_install(self, packs, group=False):
        if not group:
            self.run("dnf install -y " + packs)
        else:
            self.run("dnf groupinstall -y " + packs)

    def zypper_install(self, packs, group=False):
        self.run("zipper --non-interactive install " + packs)

    def pacman_install(self, packs, group=False):
        self.run("pacman --noconfirm -S " + packs)

    def brew_install(self, packs, group=False):
        self.run('brew install -y ' + packs)

    def install(self, packs, group=False):
        if self.os == 'linux':
            if self.dist == 'fedora':
                self.dnf_install(packs, group)
            elif self.dist == 'ubuntu' or self.dist == 'debian':
                self.apt_install(packs, group)
            elif self.dist == 'centos' or self.dist == 'redhat':
                self.yum_install(packs, group)
            elif self.dist == 'suse':
                self.zypper_install(packs, group)
            elif self.dist == 'arch':
                self.pacman_install(packs, group)
            else:
                Assert(False), "Cannot determine installer"
        elif self.os == 'macosx':
            self.brew_install(packs, group)
        else:
            Assert(False), "Cannot determine installer"

    def group_install(self, packs):
        self.install(packs, group=True)

    #------------------------------------------------------------------------------------------

    def pip_install(self, cmd):
        self.run("pip install " + cmd)

    def pip3_install(self, cmd):
        self.run("pip3 install " + cmd)

    def setup_pip(self):
        get_pip = "set -e; cd /tmp; curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py"
        if not self.has_command("pip"):
            self.install("curl")
            self.run(get_pip + "; python2 get-pip.py")
        ## fails on ubuntu 18:
        # if not has_command("pip3") and has_command("python3"):
        #     run(get_pip + "; python3 get-pip.py")
