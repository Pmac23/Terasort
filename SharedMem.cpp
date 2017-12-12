#include <bits/stdc++.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <iterator>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <malloc.h>
#include <string.h>
#include <string>
#include <vector>
#include<sys/time.h>

#define TYPE int
#define MIN_LENGTH 4
#define SIZE 128*1024*1024*1024
#define RamSizeRun 1024*1024*1024
#define TheadCount 2
#define rowSize 100
#define zero 0
#define MAXASCII 99999999999999999
#define chunkDivide 128


using namespace std;
using std::string;

FILE *fi,*fo;
FILE *output;
FILE *out[chunkDivide];
char FileName[2];
long size_siChunk;
long int final_size,flag_time=0;
long int data_process_thread;
int iOut;
int  thread_count=0;
double gv_latency=0, gv_throughput=0, gv_exec_time=0;
clock_t end_max,start_min=INT_MAX;
string check_val, putString;


struct timeval start_time,end_time;

struct MinHeapNode
{
    string element;
    int i;
};


void swap(MinHeapNode* x, MinHeapNode* y);

class MinHeap
{
    MinHeapNode* harr;
    int heap_size;

public:
    MinHeap(MinHeapNode a[], int size);
    void MinHeapify(int);
    int left(int i)
    {
         return (2 * i + 1);
    }
    int right(int i)
    {
        return (2 * i + 2);
    }
    MinHeapNode getMin()
    {
        return harr[0];
    }
    void replaceMin(MinHeapNode x)
    {
        harr[0] = x;
        MinHeapify(0);
    }
};



MinHeap::MinHeap(MinHeapNode a[], int s_i)
{
    heap_size = s_i;
    harr = a; // store address of array
    int i = (heap_size - 1) / 2;
    while (i >= 0)
    {
        MinHeapify(i);
        i--;
    }
}



// Used to heapify the subtree
void MinHeap::MinHeapify(int x)
{
    int l = left(x);
    int r = right(x);
    int smallest = x;
    if (l < heap_size && harr[l].element < harr[x].element)
        smallest = l;
    if (r < heap_size && harr[r].element < harr[smallest].element)
        smallest = r;
    if (smallest != x)
    {
        swap(&harr[x], &harr[smallest]);
        MinHeapify(smallest);
    }
}


void swap(MinHeapNode* x, MinHeapNode* y)
{
    MinHeapNode temp = *x;
    *x = *y;
    *y = temp;
}


void  mergeOutput(int num_ways,int run_size)
{
    FILE* ip[num_ways];
    string mystring;
    for(int i=0;i<num_ways;i++)
    {
        char filen[2];

        //convert the integer to string
        snprintf(filen,sizeof(filen),"%d",i);

        //we open the file in input mode
        ip[i] = fopen(filen,"r");
    }

     MinHeapNode* heap_arr = new MinHeapNode[num_ways];
     int i=0;
     for (i = 0; i < num_ways; i++)
     {

        if (fscanf(ip[i], "%s ", &heap_arr[i].element) != 1)
        {
            break;
        }
        else
        {
           heap_arr[i].i = i;
        }
     }
     MinHeap hp(heap_arr, i);

    int c = 0;
    while(c != i)
    {
        MinHeapNode root = hp.getMin();
        string mystring = (string)root.element;

        if (fscanf(ip[root.i], "%s ", &root.element) != 1 )
        {
            root.element = MAXASCII;
            c++;
        }
        // in this case we replace the root of the next element
        hp.replaceMin(root);

    }

    for (int i = 0; i < num_ways; i++)
    {
        fclose(ip[i]);
    }

    fclose(fo);
}

void MyMerge(vector<string> fileVector, int start, int middle, int end1)
{
   // string opString[mystring.size()];
    int ret1=0;
    string left1,right1;
    vector <string> left,right;
    for (int l=0; l<middle-start; l++)
    {
        left.push_back(fileVector[start+l]);
    }

    for(int r=0; r<end1-middle+1;r++)
    {
        right.push_back(fileVector[middle+r]);
    }

    int i=0,j=0;
    for(int k=start;k<=end1;k++)
    {

       ret1 = strncmp(left[i].c_str(),right[i].c_str(),10);
       if(ret1 < 0)
       {
           fileVector[k] = left[i];
           i++;
       }
       else
       {
            fileVector[k] = right[j];
            j++;
       }
 }

}



void MyMergesort(vector<string> fileVector,int start, int end1)
{

    if(start<end1)
    {
        long middle = (end1 + start)/ 2;

		// Sort first and second halves
		MyMergesort(fileVector, start, middle);
		MyMergesort(fileVector, middle + 1, end1);
		MyMerge(fileVector, start, middle+1, end1);
    }
    }


void *DivideSort(void *attr)
{
    char* sortData = (char*)malloc(RamSizeRun*sizeof(char));
    printf("ftell: %ld\n", ftell(fi));
    int i=0,flag=0,endCount=0;
    size_t nbytes;
    string *MergeData = new string[52];
    string temp = " ";
    char* Byte100 = new char[100*sizeof(char)];
    vector <string> fileVector;
    char mystring [100],myoutput[100];
    long sizeCount = 0;

    size_t fsize2;

        while(!feof(fi))
        {
            if (fgets(mystring, sizeof(char)*100, fi) != NULL)
            {
               fileVector.push_back(mystring);
               endCount++;
               sizeCount = sizeCount + sizeof(char)*100;
            }
            else
            {
                  puts("Error2 or EOF");
                break;
            }

            if(sizeCount >=size_siChunk)
            {
                cout<<"call sort";
                MyMergesort(fileVector,zero,endCount);

                // write the output to the file
                char fileName[2];

                // convert i to string
                snprintf(fileName, sizeof(fileName), "%d", iOut);

                // Open output files in read mode.
                out[iOut] = fopen(fileName, "w");
                iOut ++;
                fwrite (mystring , sizeof(char), sizeof(mystring), out[iOut]);
                sizeCount = 0;
                memset(mystring, 0, sizeof(mystring));
                fileVector.clear();

            }

    }

}

void TeraSortMain(int num_ways, int run_size)
{
    int MegaChunks = num_ways/2;
    pthread_t  p_thread[2];

    // Create the chunks
    for(int i=0; i<MegaChunks;i++)
    {
            // run one MegaChunk which holds two chunk at a time, this will be passed in the RAM in parallel
            for(int j=0; j< 1 ; j++)
            {
                pthread_create(&p_thread[j],NULL,DivideSort,NULL);
            }

            for(int j=0; j< 1 ; j++)
            {
                pthread_join(p_thread[j],NULL);
            }
    }

}

void externalSort(int num_ways,int run_size)
{
    // read from disk, create chunks, sort and write output to temporary files
    TeraSortMain(num_ways, run_size);

    // read data from output files and merge them into final file
    mergeOutput(num_ways,run_size);
}

// Driver program to test above
int main()
{
	// No. of Partitions of input file.
	int num_ways;
    iOut = 0;
    double d1,d2;
	// The size of each partition
	int run_size = RamSizeRun;

	// Open a file in write mode
    fi = fopen("Input", "r");
    fo = fopen("Output.txt", "w");


    float chunks = SIZE/RamSizeRun;
    size_siChunk = SIZE/RamSizeRun;
    num_ways = (int) chunks;
    printf("chunk %f\n", num_ways);

    gettimeofday(&start_time, NULL );
	externalSort(num_ways,run_size);
	gettimeofday(&end_time, NULL );

	d1 = (double)start_time.tv_sec + ((double)start_time.tv_usec/1000000);
	d2 = (double)end_time.tv_sec + ((double)end_time.tv_usec/1000000);

	gv_exec_time = d2 - d1;

	printf("Total time for sorting: %f",gv_exec_time);

	return 0;
}

