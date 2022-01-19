#ifndef PLUGINS_PYTHON_REDISGEARS_PYTHON_H_
#define PLUGINS_PYTHON_REDISGEARS_PYTHON_H_

#define REDISGEARSPYTHON_PLUGIN_NAME "GearsPythonPlugin"

#define REDISGEARSPYTHON_VERSION_MAJOR 1
#define REDISGEARSPYTHON_VERSION_MINOR 0
#define REDISGEARSPYTHON_VERSION_PATCH 0

#define STR1(a) #a
#define STR(e) STR1(e)

#define REDISGEARSPYTHON_PLUGIN_VERSION \
  (REDISGEARSPYTHON_VERSION_MAJOR * 10000 + REDISGEARSPYTHON_VERSION_MINOR * 100 + REDISGEARSPYTHON_VERSION_PATCH)


#endif /* PLUGINS_PYTHON_REDISGEARS_PYTHON_H_ */
