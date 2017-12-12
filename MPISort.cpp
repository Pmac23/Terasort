#include "mpi.h"
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






// Driver program to test above
int main(int argc, char** argv)
{
    int n = atoi(argv[1]);
	//char *file = argv[2];
	// No. of Partitions of input file.
	int num_ways;
    iOut = 0;
    double d1,d2;
	// The size of each partition
	int run_size = RamSizeRun;

	// Open a file in write mode
    fi = fopen(argv[2], "r");
    fo = fopen("Output.txt", "w");

    int world_rank;
	int world_size;

	MPI_INIT(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	int size1 = SIZE / RamSizeRun;
	size1 = size1/world_size;
    float chunks = SIZE/RamSizeRun;
    size_siChunk = SIZE/RamSizeRun;
    num_ways = (int) chunks;
    printf("chunk %f\n", num_ways);
    //char*sub_array = malloc(size1 * sizeof(char));
    gettimeofday(&start_time, NULL );
    for(int i=0;i<chunks;i++)
    {
        char *original_array = malloc(chunks*sizeof(char));
        char*sub_array = malloc(size1 * sizeof(char));
        fgets(original_array, sizeof(char)*100, fi);
        MPI_Scatter(original_array, size1, MPI_INT, sub_array, size1, MPI_INT, 0, MPI_COMM_WORLD);
        char *tmp_array = malloc(size * sizeof(char));
        mergeSort(sub_array, tmp_array, 0, (size - 1));

        int *sorted = NULL;
        if (world_rank == 0) {

            sorted = malloc(n * sizeof(int));

        }

        MPI_Gather(sub_array, size, MPI_INT, sorted, size, MPI_INT, 0, MPI_COMM_WORLD);



		free(sorted);
		free(other_array);
		 if (world_rank == 0) {

            char *other_array = malloc(n * sizeof(char));
            MyMergeSort(sorted, other_array, 0, (n - 1));
        }
    }







    mergeOutput(chunks,RamSizeRun);


	free(original_array);
	free(sub_array);
	free(tmp_array);


	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();


	//externalSort(num_ways,run_size);
	gettimeofday(&end_time, NULL );

	d1 = (double)start_time.tv_sec + ((double)start_time.tv_usec/1000000);
	d2 = (double)end_time.tv_sec + ((double)end_time.tv_usec/1000000);

	gv_exec_time = d2 - d1;

	printf("Total time for sorting: %f",gv_exec_time);

	return 0;
}

