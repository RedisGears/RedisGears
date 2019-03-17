/* adlist.h - A generic doubly linked list implementation
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

typedef struct Gears_listNode {
    struct Gears_listNode *prev;
    struct Gears_listNode *next;
    void *value;
} Gears_listNode;

typedef struct Gears_listIter {
    Gears_listNode *next;
    int direction;
} Gears_listIter;

typedef struct Gears_list {
    Gears_listNode *head;
    Gears_listNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned long len;
} Gears_list;

/* Functions implemented as macros */
#define Gears_listLength(l) ((l)->len)
#define Gears_listFirst(l) ((l)->head)
#define Gears_listLast(l) ((l)->tail)
#define Gears_listPrevNode(n) ((n)->prev)
#define Gears_listNextNode(n) ((n)->next)
#define Gears_listNodeValue(n) ((n)->value)

#define Gears_listSetDupMethod(l,m) ((l)->dup = (m))
#define Gears_listSetFreeMethod(l,m) ((l)->free = (m))
#define Gears_listSetMatchMethod(l,m) ((l)->match = (m))

#define Gears_listGetDupMethod(l) ((l)->dup)
#define Gears_listGetFree(l) ((l)->free)
#define Gears_listGetMatchMethod(l) ((l)->match)

/* Prototypes */
Gears_list *Gears_listCreate(void);
void Gears_listRelease(Gears_list *list);
void Gears_listEmpty(Gears_list *list);
Gears_list *Gears_listAddNodeHead(Gears_list *list, void *value);
Gears_list *Gears_listAddNodeTail(Gears_list *list, void *value);
Gears_list *Gears_listInsertNode(Gears_list *list, Gears_listNode *old_node, void *value, int after);
void Gears_listDelNode(Gears_list *list, Gears_listNode *node);
Gears_listIter *Gears_listGetIterator(Gears_list *list, int direction);
Gears_listNode *Gears_listNext(Gears_listIter *iter);
void Gears_listReleaseIterator(Gears_listIter *iter);
Gears_list *Gears_listDup(Gears_list *orig);
Gears_listNode *Gears_listSearchKey(Gears_list *list, void *key);
Gears_listNode *Gears_listIndex(Gears_list *list, long index);
void Gears_listRewind(Gears_list *list, Gears_listIter *li);
void Gears_listRewindTail(Gears_list *list, Gears_listIter *li);
void Gears_listRotate(Gears_list *list);
void Gears_listJoin(Gears_list *l, Gears_list *o);

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
