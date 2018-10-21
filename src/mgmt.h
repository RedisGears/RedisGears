/*
 * mgmt.h
 *
 *  Created on: Oct 16, 2018
 *      Author: meir
 */

#ifndef SRC_MGMT_H_
#define SRC_MGMT_H_

#include <stdbool.h>
#include "redistar.h"

bool FiltersMgmt_Add(char* name, RediStar_FilterCallback callback);
RediStar_FilterCallback FiltersMgmt_Get(char* name);

bool MapsMgmt_Add(char* name, RediStar_MapCallback callback);
RediStar_MapCallback MapsMgmt_Get(char* name);

bool ReadersMgmt_Add(char* name, RediStar_ReaderCallback callback);
RediStar_ReaderCallback ReadersMgmt_Get(char* name);

bool WritersMgmt_Add(char* name, RediStar_WriterCallback callback);
RediStar_WriterCallback WritersMgmt_Get(char* name);

bool ExtractorsMgmt_Add(char* name, RediStar_ExtractorCallback callback);
RediStar_ExtractorCallback ExtractorsMgmt_Get(char* name);

bool ReducersMgmt_Add(char* name, RediStar_ReducerCallback callback);
RediStar_ReducerCallback ReducersMgmt_Get(char* name);

void Mgmt_Init();


#endif /* SRC_MGMT_H_ */
