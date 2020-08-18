test_assign4: test_assign4_1.c btree_mgr.c btree_mgr.h record_mgr.c record_mgr.h expr.c expr.h tables.h test_expr.c rm_serializer.c buffer_mgr.c buffer_mgr.h buffer_mgr_stat.c buffer_mgr_stat.h storage_mgr.c storage_mgr.h dt.h test_helper.h dberror.c dberror.h btree_helper.h btree_helper.c
	gcc -w -I. -c -o btree_helper.o btree_helper.c
	gcc -w -I. -c -o btree_mgr.o btree_mgr.c
	gcc -w -I. -c -o storage_mgr.o storage_mgr.c
	gcc -w -I. -c -o buffer_mgr.o buffer_mgr.c
	gcc -w -I. -c -o buffer_mgr_stat.o buffer_mgr_stat.c
	gcc -w -I. -c -o test_assign4_1.o test_assign4_1.c
	gcc -w -I. -c -o dberror.o dberror.c
	gcc -w -I. -c -o expr.o expr.c
	gcc -w -I. -c -o record_mgr.o record_mgr.c
	gcc -w -I. -c -o rm_serializer.o rm_serializer.c
	gcc -w -I. -c -o test_expr.o test_expr.c
	gcc -w -I. -o test_assign4 test_assign4_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o btree_mgr.o rm_serializer.o test_expr.o btree_helper.o

clean:
	rm -rf *.o test_assign4

all: test_assign4
