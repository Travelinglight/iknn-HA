#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "funcapi.h"
 
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(iknnHA);

typedef struct {
    char *fieldName;
    float value;
    char complete;
}queryObj;

typedef struct {
    char **vals;
    char *type; // n: null, i: int, f: float
    double dist;
}HeapRec;

typedef struct {
    HeapRec *rec;
    long size;
    long length;
}Heap;

int dim;    // dimension
Heap res;   // result

double fabs(double n);
double string2double(char *number);
void chopQueryFieldNames(char *fieldNames, char **qFnames);
void chopQueryValues(char *queryValues, float *qValues, int nQueryFields);
void digestQuery(char *iknnQuery, char *tbl, char **qFnames, float *qValues, int *K, int nQueryFields);
void heapInsert(Heap *heap, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc);
void heapCover(Heap *heap, long node, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc);
int binarySearch(int maxBin, char* fieldName, char* tb, float qVal);
double calcDist(TupleDesc *tupdesc, SPITupleTable *tuptable, long pt, queryObj *qObj);
void extractVals(SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, HeapRec *rec);

double fabs(double n) {
    return n < 0 ? -n : n;
}

double string2double(char *number) {
    int i, flag = 1;    // flag=1 before point, flag=0 after point
    long div10 = 1, sign = 1;
    double k = 0;
    for (i = 0; i < strlen(number); i++) {
        if (i == 0 && number[i] == '-') {
            sign = -1;
        }
        else if (number[i] == '.') {
            flag = 0;
            div10 *= 10;
        }
        else if (number[i] >= '0' && number[i] <= '9') {
            k = k * (flag > 0 ? 10 : 1) + (float)(number[i] - '0') / (flag > 0 ? 1 : div10);
            if (flag == 0)
                div10 *= 10;
        }
        else {
            // error handling
        }
    }
    return k * sign;
}

// chop query field names into a string array
void chopQueryFieldNames(char *fieldNames, char **qFnames) {
    int i, j;
    i = 0;
    while ((fieldNames != NULL) && (strlen(fieldNames) > 0)) {
        for (j = 0; j < strlen(fieldNames); j++) {
            if (fieldNames[j] == ',')
                break;
            qFnames[i][j] = fieldNames[j];
        }
        qFnames[i++][j] = '\0';
        while (qFnames[i - 1][0] == ' ')
            strcpy(qFnames[i - 1], qFnames[i - 1] + 1);
        while (qFnames[i - 1][strlen(qFnames[i - 1]) - 1] == ' ')
            strncpy(qFnames[i - 1], qFnames[i - 1], strlen(qFnames[i - 1]) - 1);
        if (j < strlen(fieldNames) && fieldNames[j] == ',')
            strcpy(fieldNames, fieldNames + j + 1);
        else
            break;
    }
}

// chop query values into an int array
void chopQueryValues(char *queryValues, float *qValues, int nQueryFields) {
    int nQV = 0;    // number of query values
    int i, j;
    char *number;
    number = (char*)palloc0(sizeof(char) * 256);

    for (i = 0; i < strlen(queryValues); i++)
        if (queryValues[i] == ',')
            nQV++;
    i = 0;
    while ((queryValues != NULL) && (strlen(queryValues) > 0)) {
        memset(number, 0, 256);
        for (j = 0; j < strlen(queryValues); j++) {
            if (queryValues[j] == ',')
                break;
            number[j] = queryValues[j];
        }

        number[j] = '\0';
        while (number[0] == ' ')
            strcpy(number, number + 1);
        while (number[strlen(number) - 1] == ' ')
            strncpy(number, number, strlen(number) - 1);

        qValues[i++] = string2double(number);
        if (j < strlen(queryValues) && queryValues[j] == ',')
            strcpy(queryValues, queryValues + j + 1);
        else
            break;
    }
}

void digestQuery(char *iknnQuery, char *tbl, char **qFnames, float *qValues, int *K, int nQueryFields) {
//    find 3 nearest neighbour of (a, b, c)(1, 2, 3) from hash
    char *number;
    char *tmpQuery;
    char *fieldNames;
    char *queryValues;
    tmpQuery = (char*)palloc0(strlen(iknnQuery) * sizeof(char));

    // get K
    strcpy(tmpQuery, strchr(iknnQuery, ' ') + 1);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    number = (char*)palloc0((strchr(tmpQuery, ' ') - tmpQuery + 1) * sizeof(char));
    strncpy(number, tmpQuery, strchr(tmpQuery, ' ') - tmpQuery);
    *K = (int)string2double(number);
    
    // get query field names
    strcpy(tmpQuery, strchr(tmpQuery, '(') + 1);
    fieldNames = (char*)palloc0((strchr(tmpQuery, ')') - tmpQuery + 1) * sizeof(char));
    strncpy(fieldNames, tmpQuery, strchr(tmpQuery, ')') - tmpQuery);
    chopQueryFieldNames(fieldNames, qFnames);

    // get query values
    strcpy(tmpQuery, strchr(tmpQuery, '(') + 1);
    queryValues = (char*)palloc0((strchr(tmpQuery, ')') - tmpQuery + 1) * sizeof(char));
    strncpy(queryValues, tmpQuery, strchr(tmpQuery, ')') - tmpQuery);
    chopQueryValues(queryValues, qValues, nQueryFields);

    // get table name
    strcpy(tmpQuery, strstr(tmpQuery, "from ") + 5);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    while (tmpQuery[strlen(tmpQuery) - 1] == ' ')
        strncpy(tmpQuery, tmpQuery, strlen(tmpQuery) - 1);
    tmpQuery[strlen(tmpQuery)] = '\0';
    strcpy(tbl, tmpQuery);
}

void heapInsert(Heap *heap, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc) {
    long p, f;
    HeapRec tmpRec;

    // insert a new object
    heap->rec[++heap->length].dist = dist;
    extractVals(tuptable, pt, tupdesc, &heap->rec[heap->length]); 

    // percolate up
    p = heap->length;
    while ((p >> 1) > 0) {
        f = p >> 1;
        if (heap->rec[f].dist < heap->rec[p].dist) {
            tmpRec = heap->rec[p];
            heap->rec[p] = heap->rec[f];
            heap->rec[f] = tmpRec;
        }
        p = f;
    }
}

void heapCover(Heap *heap, long node, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc) {
    long p = 1, s;
    HeapRec tmpRec;

    // cover the top object
    heap->rec[1].dist = dist;
    extractVals(tuptable, pt, tupdesc, &heap->rec[1]);

    // percolate down
    while ((p << 1) <= heap->length) {
        s = p << 1;
        if ((s < heap->length) && (heap->rec[s].dist < heap->rec[s+1].dist))
            s++;
        if (heap->rec[p].dist < heap->rec[s].dist) {
            tmpRec = heap->rec[p];
            heap->rec[p] = heap->rec[s];
            heap->rec[s] = tmpRec;
        }
        p = s;
    }
}

int binarySearch(int maxBin, char* fieldName, char* tb, float qVal) {
    int l = 1, r = maxBin;
    long proc;
    float midVal;
    char *cmd;
    int ret;
    TupleDesc tupdesc;
    HeapTuple tuple;
 
    cmd = (char*)palloc0(sizeof(char) * 1024);
    while (l <= r) {
        int mid = (l + r) >> 1;
        snprintf(cmd, 1024, "SELECT * FROM habin_%s_%s_%d ORDER BY val LIMIT 1", tb, fieldName, mid);
        // run the SQL command 
        ret = SPI_exec(cmd, 1); 
        proc = SPI_processed;
        if (proc == 0) {
            r = mid - 1;
            continue;
        }
        if (ret > 0 && SPI_tuptable != NULL) {
            tupdesc = SPI_tuptable->tupdesc;
            tuple = SPI_tuptable->vals[0];
            midVal = string2double(SPI_getvalue(tuple, tupdesc, 1));
        }
        else {
            // error handling
        }

        if (midVal > qVal) {
            r = mid - 1;
            continue;
        }
        else if (midVal < qVal) {
            l = mid + 1;
            continue;
        }
        else
            return mid;
    }

    if (r == 0)
        return 1;
    if (l == maxBin + 1)
        return maxBin;

    // adjust to the bin which contains the query value
    while (l < maxBin) {
        snprintf(cmd, 1024, "SELECT * FROM habin_%s_%s_%d ORDER BY val DESC LIMIT 1", tb, fieldName, l);
        // run the SQL command 
        ret = SPI_exec(cmd, 1); 
        proc = SPI_processed;
        if (proc == 0)
            break;
        if (ret > 0 && SPI_tuptable != NULL) {
            tupdesc = SPI_tuptable->tupdesc;
            tuple = SPI_tuptable->vals[0];
            if (string2double(SPI_getvalue(tuple, tupdesc, 1)) < qVal)
                l++;
            else
                break;
        }
        else {
            // error handling
        }
    }
    while (l > 1) {
        snprintf(cmd, 1024, "SELECT * FROM habin_%s_%s_%d ORDER BY val LIMIT 1", tb, fieldName, l);
        // run the SQL command 
        ret = SPI_exec(cmd, 1); 
        proc = SPI_processed;
        if (proc == 0) {
            l--;
            continue;
        }
        if (ret > 0 && SPI_tuptable != NULL) {
            tupdesc = SPI_tuptable->tupdesc;
            tuple = SPI_tuptable->vals[0];
            if (string2double(SPI_getvalue(tuple, tupdesc, 1)) > qVal)
                l--;
            else
                break;
        }
        else {
            // error handling
        }
    }
    return l;
}

double calcDist(TupleDesc *tupdesc, SPITupleTable *tuptable, long pt, queryObj *qObj) {
    int i, Iseto = 0;
    char *number;
    double sum = 0;
    double dif;
    HeapTuple tuple = tuptable->vals[pt];

    for (i = 1; i <= dim; i++) {
        number = SPI_getvalue(tuple, *tupdesc, i + 1);
        if (number && qObj[i - 1].complete == 1) {
            Iseto++;
            dif = string2double(number) - qObj[i - 1].value;
            sum += dif * dif;
        }
    }
    return sum * dim / Iseto;
}

// extract values from tuple to array
void extractVals(SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, HeapRec *rec) {
    int i;
    char *number;
    HeapTuple tuple = tuptable->vals[pt];

    strcpy(rec->type, "");
    for (i = 1; i <= dim; i++) {
        number = SPI_getvalue(tuple, *tupdesc, i + 1);
        if (number) {
            strcpy(rec->vals[i - 1], number);
            strcat(rec->type, "1");
        }
        else
            strcat(rec->type, "0");
    }
}

Datum
iknnHA(PG_FUNCTION_ARGS) {
    char *iknnQuery;
    char *tbl;
    char getBins[1024], getFields[1024];
    char **qFnames; // query field names
    float *qValues;
    int nQueryFields = 0;
    int K, qi, i, j;
    int ret;
    long procBin, procFld;
    queryObj *qObj;

    int call_cntr;
    int max_calls;
    AttInMetadata *attinmeta;
    FuncCallContext *funcctx;
    TupleDesc tupdesc;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        // create a function context for cross-call persistence
        funcctx = SRF_FIRSTCALL_INIT();
        // switch to memory context appropriate for multiple function calls
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        ///////////////////////////////////////////////////////////////////////
        //                          User Code
        //////////////////////////////////////////////////////////////////////

        // get arguments, convert command to C string
        iknnQuery = text_to_cstring(PG_GETARG_TEXT_P(0));
        
        // find out the number of complete fields 
        for (i = strchr(iknnQuery, '(') - iknnQuery + 1; i < strchr(iknnQuery, ')') - iknnQuery - 1; i++)
            if (iknnQuery[i] == ',')
                nQueryFields++;
        qFnames = (char**)palloc0(sizeof(char*) * (++nQueryFields));
        qValues = (float*)palloc0(sizeof(float) * nQueryFields);
        for (i = 0; i < nQueryFields; i++)
            qFnames[i] = (char*)palloc0(sizeof(char) * 256);
        tbl = (char*)palloc0(sizeof(char) * 256);
        digestQuery(iknnQuery, tbl, qFnames, qValues, &K, nQueryFields);
        
        // initialize heap: first step
        res.size = 1;
        while (res.size < K + 1)
            res.size <<= 1;
        res.rec = (HeapRec*)palloc0(sizeof(HeapRec) * res.size);
        res.length = 0;
    
        // open internal connection 
        SPI_connect();
    
    // format query object
        // construct field name fetching command
        strcpy(getFields, "SELECT column_name FROM information_schema.columns WHERE table_name = '");
        strcat(getFields, tbl);
        strcat(getFields, "';");
        // run the SQL command 
        ret = SPI_exec(getFields, 4294967296); 
        // save the number of rows
        procFld = SPI_processed;
        if (ret > 0 && SPI_tuptable != NULL) {
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            SPITupleTable *tuptable = SPI_tuptable;
    
            dim = procFld - 1;
            // initialize query Object
            qObj = (queryObj*)palloc0(sizeof(queryObj) * dim);
            for (i = 0; i < dim; i++)
                qObj[i].fieldName = (char*)palloc0(sizeof(char) * 256);
            // initialize heap: second step
            for (i = 0; i < res.size; i++) {
                res.rec[i].vals = (char**)palloc0(sizeof(char*) * dim);
                for (j = 0; j < dim; j++)
                    res.rec[i].vals[j] = (char*)palloc0(sizeof(char) * 32);
                res.rec[i].type = (char*)palloc(sizeof(char) * dim);
            }

            // fill in the query Object Field names
            for (i = 0; i < procFld; i++) {
                HeapTuple tuple = tuptable->vals[i];
                // skip the additional column added during initialization
                if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "ha_id") == 0) continue;
                strcpy(qObj[i].fieldName, SPI_getvalue(tuple, tupdesc, 1));
            }

            // fill in the query Object values
            for (i = 0; i < dim; i++) {
                for (j = 0; j < nQueryFields; j++) {
                    if (strcmp(qFnames[j], qObj[i].fieldName) == 0) {
                        qObj[i].value = qValues[j];
                        qObj[i].complete = 1;
                        break;
                    }
                }
                if (j >= nQueryFields)
                    qObj[i].complete = 0;
            }
        }
        else {
            // error handle
        }
   
    // start query scanning 
        for (qi = 0; qi < dim; qi++) {
            if (qObj[qi].complete == 1) {
                snprintf(getBins, 1024, "SELECT * FROM %s_hatmp WHERE dimension = '%s'", tbl, qObj[qi].fieldName);
                // run the SQL command 
                ret = SPI_exec(getBins, 4294967296); 
                // save the number of rows
                procBin = SPI_processed;
                if (ret > 0 && SPI_tuptable != NULL) {
                    TupleDesc tupdesc = SPI_tuptable->tupdesc;
                    HeapTuple tuple = SPI_tuptable->vals[0];
                    int maxBin = (int)string2double(SPI_getvalue(tuple, tupdesc, 2));
                    long nObj = (long)string2double(SPI_getvalue(tuple, tupdesc, 3));
                    long procObj;
                    int whichBin;
                    char getObjects[1024];
                    if (nObj == 0)
                        continue;
                    whichBin = binarySearch(maxBin, qObj[qi].fieldName, tbl, qObj[qi].value);
                    snprintf(getObjects, 1024, "SELECT * FROM %s t1 NATURAL JOIN habin_%s_%s_%d t2 WHERE t1.ha_id = t2.ha_id;", tbl, tbl, qObj[qi].fieldName, whichBin);

                    // run the SQL command 
                    ret = SPI_exec(getObjects, 4294967296);
                    // save the number of rows
                    procObj = SPI_processed;
                    if (ret > 0 && SPI_tuptable != NULL) {
                        TupleDesc tupdesc = SPI_tuptable->tupdesc;
                        SPITupleTable *tuptable = SPI_tuptable;
                        long i;
                        double dist;

                        for (i = 0; i < procObj; i++) {
                            if (res.length < K) {
                                dist = calcDist(&tupdesc, tuptable, i, qObj); // -1 means no partial distance pruning
                                heapInsert(&res, dist, tuptable, i, &tupdesc);
                            }
                            else {
                                dist = calcDist(&tupdesc, tuptable, i, qObj);
                                if (dist < res.rec[1].dist)
                                    heapCover(&res, 1, dist, tuptable, i, &tupdesc);
                            }
                        }
                    }
                }
            }
        }


        ///////////////////////////////////////////////////////////////////////
        //                         /User Code
        //////////////////////////////////////////////////////////////////////
        
        // get tuple descriptor for output
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("function returning record called in context "
                            "that cannot accept type record")));

        SPI_finish();

        // total number of tuples to be returned
        funcctx->max_calls = res.length;
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        // restore memory context
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls) {
        char **values;
        HeapTuple tuple;
        Datum result;

        values = (char**)palloc((dim + 1) * sizeof(char*));
        for (i = 0; i < dim; i++) {
            values[i] = (char*)palloc(32 * sizeof(char));
            if (res.rec[call_cntr + 1].type[i] == '1')
                snprintf(values[i], 32, "%s", res.rec[call_cntr + 1].vals[i]);
            else
                values[i] = NULL;
        }
        values[dim] = (char*)palloc(32 * sizeof(char));
        snprintf(values[dim], 32, "%lf", res.rec[call_cntr + 1].dist);

        // build a tuple
        tuple = BuildTupleFromCStrings(attinmeta, values);
        // make the tuple into a datum
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}
