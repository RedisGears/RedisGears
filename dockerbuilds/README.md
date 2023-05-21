# Building

Please note - this requires **docker buildx** to be installed and configured. That is beyond the scope of this document.

Similarly, this set of tools requires python>=3.7 and jinja2

To build for a platform:

1. Go to the root of the repository
2. Generate the docker file based on your necessary options:
    ```python dockerbuilds/generate_dockerfile.py --help```
3. Build, via the generated docker file:
    ```docker buildx build -f dockerbuilds/dockerfile.generated .```

There are various build options that can be passed to even the 
generated docker file. See the specific template file for more information.

## Supported Platform matrix

| OS | Architecture | Template |
|----|--------------|----------|
| Ubuntu 22.04 | x86_64 | dockerfile.debian.tmpl |
| Ubuntu 20.04 | x86_64 | dockerfile.debian.tmpl |
| Ubuntu 18.04 | x86_64 | dockerfile.debian.tmpl |
