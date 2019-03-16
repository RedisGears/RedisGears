/*
 * globals.h
 *
 *  Created on: 2 Dec 2018
 *      Author: root
 */

#ifndef SRC_GLOBALS_H_
#define SRC_GLOBALS_H_

#include <stdbool.h>

typedef struct Globals{
    bool redisAILoaded;
    bool rediSearchLoaded;
}Globals;

extern Globals globals;

#endif /* SRC_GLOBALS_H_ */
