
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
TARGET=$(BINDIR)/libpython3.7m-fixed.a

MK_ALL_TARGETS=bindirs build pyenv
MK_CLEAN_ALL_DIRS += $(CPYTHON_PREFIX)

REDISLABS_DIR=/opt/redislabs
export CPYTHON_PREFIX=$(REDISLABS_DIR)/lib/modules/python3
PYENV_DIR=$(CPYTHON_PREFIX)/.venv

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
	--prefix=$(CPYTHON_PREFIX)

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

# pyenv: $(REDISLABS_DIR) $(CPYTHON_PREFIX) $(PYENV_DIR)
pyenv: redislabs-dir cpython-dir pyenv-dir

ifeq ($(OS),macosx)
redislabs-dir:
	$(SHOW)set -e ;\
	if [ ! -d $(REDISLABS_DIR) ]; then \
		sudo mkdir -p $(REDISLABS_DIR); \
		sudo chown $$USER $(REDISLABS_DIR); \
	fi

#redislabs-dir:
# 	$(SHOW)mkdir -p $(REDISLABS_DIR)
# 	$(SHOW)sudo ln -sfn $(abspath $(REDISLABS_DIR)) $(REDISLABS_DIR)
else
ifeq ($(WITHSUDO),1)
redislabs-dir:
	$(SHOW) sudo rm -rf $(REDISLABS_DIR)
	$(SHOW) sudo mkdir -p $(REDISLABS_DIR)
	$(SHOW) sudo chown $$USER $(REDISLABS_DIR)
else
redislabs-dir:
	$(SHOW) rm -rf $(REDISLABS_DIR)
	$(SHOW) mkdir -p $(REDISLABS_DIR)
endif
endif

cpython-dir: redislabs-dir
	@echo Installing Python...
	$(SHOW)make -C $(CONFIGURE_BUILD_DIR) install 2>&1 > $(abspath $(CONFIGURE_BUILD_DIR))/python-install.log

pyenv-dir: redislabs-dir cpython-dir
	$(SHOW)cp Pipfile* $(CPYTHON_PREFIX)
	$(SHOW)cd $(CPYTHON_PREFIX); \
	export PIPENV_VENV_IN_PROJECT=1; \
	export LC_ALL=C.UTF-8; \
	export LANG=C.UTF-8; \
	pipenv --site-packages install --python $(CPYTHON_PREFIX)/bin/python3
	$(SHOW)cp $(CPYTHON_PREFIX)/Pipfile.lock .
	$(SHOW)echo /usr/local/lib/python3.7/site-packages > $(PYENV_DIR)/lib/python3.7/site-packages/local.pth
