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

typedef struct JVM_listNode {
    struct JVM_listNode *prev;
    struct JVM_listNode *next;
    void *value;
} JVM_listNode;

typedef struct JVM_listIter {
    JVM_listNode *next;
    int direction;
} JVM_listIter;

typedef struct JVM_list {
    JVM_listNode *head;
    JVM_listNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned long len;
} JVM_list;

/* Functions implemented as macros */
#define JVM_listLength(l) ((l)->len)
#define JVM_listFirst(l) ((l)->head)
#define JVM_listLast(l) ((l)->tail)
#define JVM_listPrevNode(n) ((n)->prev)
#define JVM_listNextNode(n) ((n)->next)
#define JVM_listNodeValue(n) ((n)->value)

#define JVM_listSetDupMethod(l,m) ((l)->dup = (m))
#define JVM_listSetFreeMethod(l,m) ((l)->free = (m))
#define JVM_listSetMatchMethod(l,m) ((l)->match = (m))

#define JVM_listGetDupMethod(l) ((l)->dup)
#define JVM_listGetFree(l) ((l)->free)
#define JVM_listGetMatchMethod(l) ((l)->match)

/* Prototypes */
JVM_list *JVM_listCreate(void);
void JVM_listRelease(JVM_list *list);
void JVM_listEmpty(JVM_list *list);
JVM_list *JVM_listAddNodeHead(JVM_list *list, void *value);
JVM_list *JVM_listAddNodeTail(JVM_list *list, void *value);
JVM_list *JVM_listInsertNode(JVM_list *list, JVM_listNode *old_node, void *value, int after);
void JVM_listDelNode(JVM_list *list, JVM_listNode *node);
JVM_listIter *JVM_listGetIterator(JVM_list *list, int direction);
JVM_listNode *JVM_listNext(JVM_listIter *iter);
void JVM_listReleaseIterator(JVM_listIter *iter);
JVM_list *JVM_listDup(JVM_list *orig);
JVM_listNode *JVM_listSearchKey(JVM_list *list, void *key);
JVM_listNode *JVM_listIndex(JVM_list *list, long index);
void JVM_listRewind(JVM_list *list, JVM_listIter *li);
void JVM_listRewindTail(JVM_list *list, JVM_listIter *li);
void JVM_listRotate(JVM_list *list);
void JVM_listJoin(JVM_list *l, JVM_list *o);

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
