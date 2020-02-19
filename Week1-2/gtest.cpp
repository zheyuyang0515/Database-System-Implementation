#include <gtest/gtest.h>
#include "DBFile.h"
TEST(DBFile, Create)
{
    DBFile dbfile;
    fType type;
    type = heap;
    ASSERT_EQ(1, dbfile.Create("dbfile", type, NULL));
}
TEST(DBFile, Open)
{
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Open("dbfile"));
}
TEST(DBFile, Close)
{
    DBFile dbfile;
    ASSERT_EQ(1, dbfile.Close());
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}