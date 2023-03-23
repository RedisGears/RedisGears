- RedisGears Issue: [RedisGears/RedisGears#873](https://github.com/RedisGears/RedisGears/pull/873)

# RedisGears JavaScript API Versioning System

This is an RFC to allow the user code sent to RedisGears javascript engine
be verified for correct use of the APIs and to follow the semantic versioning
of the API.

# Motivation

Due to possible changes within the JS API of RedisGears, sometimes it is
impossible to avoid breaking changes or changes which don't break
compatibility, but just add something new instead.

A breaking change is a change which is incompatible with the API it was
based on, for example:

- Changing the function semantics (what it does).
- Changing the function prototype (definition) - name, arguments, return
value.

A non-breaking change allows the API it is based on to continue correct
operation with all the previously defined semantics.

To let the users avoid this headache, this RFC suggests having the API
versioned following the Semantic Versioning scheme of version 2.0, but
without having the patch version, as it doesn't make sense in case of the
JS API, in particular.

To recall, this scheme uses a scheme of `x.y.z`, where `x` is the major
version number, which changes only when there is a compatibility breakage
due to the introduction of the breaking changes, the `y` is the minor
version, which designates that there have been compatible, non-breaking
changes, and the `z` denotes a patch version number is a change not
touching the API but rather a bug fix.

# Detailed design

## Specification

Currently, there is no versioning in the JS modules loaded by RedisGears.
However, there is a mechanism allowing to specify meta-information for
the module, for example, the first line here:

```javascript
#!js name=lib

redis.register_function('foo', () => {
    return 1;
});
```

The first line `#!js name=lib` tells us the name of the module, - `lib`.

This RFC suggests having an additional, but mandatory specification of
the version right after the module name, which is somewhat similar to
[GLSL](https://www.khronos.org/opengl/wiki/Core_Language_(GLSL))(`#version 150`).
Using the proposed scheme, the first line would look like this:

```javascript
#!js name=lib api_version=1.0
```

Provided the code is written this way, it is easy to parse out the API
version number for the validator.

## Validation

The API version specified should be validated for the reasons:

- To allow the user to know that the module has successfully passed
the validation and is loaded properly by RedisGears.
- To allow the user to know that the module couldn't be loaded due to
improper use of the API: the requested version can be either unsupported
(too old), not existing yet (too early to use it, given the current
RedisGears version used), or incompatible with the code used within the
module.

The api version number checks are done as per the semantic versioning scheme,
however, the code validation has to take place as well. The validation
of code implies that the user code within the module only uses the APIs
exposed by the specified version only:

- All the APIs available in more recent (newer) versions are **not**
available to the user.
- All the APIs that have been deprecated but still exist in the current
version allow the module to pass the validation but issue a warning to
the user that these (a detailed list of APIs used but deprecated) APIs
are deprecated and are to be removed in the next major version.
- All the non-existing APIs (either due to previous removal or not
existing), are not allowed to be used and the validator rejects such code.

## Compatibility

The API versioning proposal does not break any compatibility at this
point, however, for future use, the compatibility is suggested to be:

- In version `X` an API is introduced.
- In version `X + 1`, the aforementioned API may be declared as deprecated.
- In version `X + 2`, the aforementioned API may be safely removed.

This should allow to have a long room for warning the users about the
changes and letting to change the module code adapt to the new APIs.
