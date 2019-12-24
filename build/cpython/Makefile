
# built-in rules are needed here
# MAKEFLAGS syntax differs in gnu make v3 and v4
override MAKEFLAGS:=$(filter-out r -r no-builtin-rules --no-builtin-rules,$(MAKEFLAGS))

ROOT=../..
include $(ROOT)/deps/readies/mk/main

MK.configure=1

#----------------------------------------------------------------------------------------------

BINDIR=$(BINROOT)/cpython

CONFIGURE_BUILD_DIR=$(BINDIR)

SRCDIR=$(ROOT)/deps/cpython

CONFIGURE_TARGET=$(BINDIR)/libpython3.7m.a
CONFIGURE_PYTHON=$(BINDIR)/python
TARGET=$(BINDIR)/libpython3.7m-fixed.a

MK_ALL_TARGETS=bindirs build pyenv
# MK_CLEAN_ALL_DIRS += $(REDISLABS_CPYTHON_PREFIX)

REDISLABS_DIR=/opt/redislabs
export REDISLABS_CPYTHON_PREFIX=$(REDISLABS_DIR)/lib/modules/python3
REDISLABS_LIBPYTHON=$(REDISLABS_CPYTHON_PREFIX)/lib/libpython3.7m.a
REDISLABS_PYTHON=$(REDISLABS_CPYTHON_PREFIX)/bin/python3
PYENV_DIR=$(REDISLABS_CPYTHON_PREFIX)/.venv
PYENV_PYTHON=$(PYENV_DIR)/bin/python3

include $(MK)/defs

#----------------------------------------------------------------------------------------------

export CFLAGS=\
	-fPIC \
	-Wno-nullability-completeness \
	-Wno-expansion-to-defined

ifeq ($(OS),macosx)
BINUTILS_PREFIX:=$(shell brew --prefix binutils)
OBJCOPY:=$(BINUTILS_PREFIX)/bin/objcopy
else
OBJCOPY:=objcopy
endif

#----------------------------------------------------------------------------------------------

PYTHON_ENCODING ?= ucs4

CONFIGURE_FLAGS += \
	$(if $(eq $(PYTHON_ENCODING),),,--enable-unicode=$(PYTHON_ENCODING)) \
	--prefix=$(REDISLABS_CPYTHON_PREFIX)

#----------------------------------------------------------------------------------------------

include $(MK)/rules

$(TARGET): $(CONFIGURE_TARGET)
	@echo Fixing $@ ...
	$(SHOW)cp $< $@
ifeq ($(OS),macosx)
	$(SHOW)cp $(BINDIR)/Python/pystate.o $(BINDIR)/
	$(SHOW)$(OBJCOPY) --localize-symbol _PyGILState_Ensure --localize-symbol _PyGILState_Release $(BINDIR)/pystate.o
	$(SHOW)ar rs $@ $(BINDIR)/pystate.o
	$(SHOW)rm $(BINDIR)/pystate.o
else
	$(SHOW)$(OBJCOPY) --localize-symbol PyGILState_Ensure --localize-symbol PyGILState_Release $@
endif

#----------------------------------------------------------------------------------------------

.PHONY: pyenv redislabs-dir cpython-dir pyenv-dir

# pyenv: $(REDISLABS_DIR) $(REDISLABS_CPYTHON_PREFIX) $(PYENV_DIR)
# pyenv: redislabs-dir cpython-dir pyenv-dir

# ifeq ($(OS),macosx)
# redislabs-dir:
# 	$(SHOW)set -e ;\
# 	if [ ! -d $(REDISLABS_DIR) ]; then \
# 		sudo mkdir -p $(REDISLABS_DIR); \
# 		sudo chown $$USER $(REDISLABS_DIR); \
# 	fi
# 
# #redislabs-dir:
# # 	$(SHOW)mkdir -p $(REDISLABS_DIR)
# # 	$(SHOW)sudo ln -sfn $(abspath $(REDISLABS_DIR)) $(REDISLABS_DIR)
# else
# redislabs-dir:
# 	$(SHOW)$(SUDO) rm -rf $(REDISLABS_DIR)
# 	$(SHOW)$(SUDO) mkdir -p $(REDISLABS_DIR)
# endif

# cpython-dir: redislabs-dir
$(REDISLABS_PYTHON) : $(CONFIGURE_TARGET) $(CONFIGURE_PYTHON)
	@echo Installing Python...
	$(SHOW)$(SUDO) mkdir -p $(REDISLABS_DIR)
	$(SHOW)$(SUDO) chown $$USER $(REDISLABS_DIR)
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) install 2>&1 > $(abspath $(CONFIGURE_BUILD_DIR))/python-install.log

# cpython: $(DEFAULT_TARGETS) $(REDISLABS_LIBPYTHON)

pyenv: $(PYENV_PYTHON)

# redislabs-dir cpython-dir
$(PYENV_PYTHON) : $(REDISLABS_PYTHON)
	$(SHOW)cp Pipfile* $(REDISLABS_CPYTHON_PREFIX)
	$(SHOW)set -e ;\
	cd $(REDISLABS_CPYTHON_PREFIX) ;\
	export PIPENV_VENV_IN_PROJECT=1 ;\
	export LC_ALL=C.UTF-8 ;\
	export LANG=C.UTF-8 ;\
	pipenv --site-packages install --python $(REDISLABS_PYTHON)
	$(SHOW)cp $(REDISLABS_CPYTHON_PREFIX)/Pipfile.lock .
	$(SHOW)echo /usr/local/lib/python3.7/site-packages > $(PYENV_DIR)/lib/python3.7/site-packages/local.pth
	$(SHOW)echo /usr/lib/python3/dist-packages > $(PYENV_DIR)/lib/python3.7/site-packages/global.pth 

ifeq ($(DIAG),1)
$(info *** CONFIGURE_PYTHON=$(CONFIGURE_PYTHON))
$(info *** CONFIGURE_TARGET=$(CONFIGURE_TARGET))
endif

pyenv: $(DEFAULT_TARGETS) $(PYENV_PYTHON)
