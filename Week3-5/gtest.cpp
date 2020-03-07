#include <gtest/gtest.h>
#include "BigQ.h"
#include <pthread.h>
#include <iostream>
TEST(BigQ, CreateWorkerWithNullPointer)
{
    Pipe input (100);
	Pipe output (100);
    OrderMaker orderMaker;
    BigQ b (input, output, orderMaker, 100);
    pthread_t pid;
    ASSERT_EQ(-1, b.createWorer(NULL, pid));
}
TEST(BigQ, CreateWorkerSuccess)
{
    Pipe input (100);
	Pipe output (100);
    OrderMaker orderMaker;
    BigQ b (input, output, orderMaker, 100);
    bigQstruct *s = new bigQstruct {input, output, orderMaker, 100};
    pthread_t pid;
    ASSERT_EQ(0, b.createWorer(s, pid));
}
TEST(BigQ, StringToCharArray)
{
    Pipe input (100);
	Pipe output (100);
    OrderMaker orderMaker;
    BigQ b (input, output, orderMaker, 100);
    const char * expect = "123";
    EXPECT_STREQ(expect, b.stringToChar("123"));
}

TEST(BigQ, SaveRuns)
{
    Pipe input (100);
	Pipe output (100);
    OrderMaker orderMaker;
    BigQ b (input, output, orderMaker, 100);
    File f;
    f.Open(0, b.stringToChar("gtest.tmp"));
    vector<Record*> run;
    ASSERT_EQ(1, b.saveRuns(f, run));
    f.Close();
}
int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}