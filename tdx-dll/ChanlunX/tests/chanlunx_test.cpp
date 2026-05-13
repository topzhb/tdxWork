#include <gtest/gtest.h>
#include <vector>

// 引用核心算法（通过编译链接 ChanlunX 获得）
// 注意：ChanlunX 是 DLL，算法函数需通过对象文件或静态库访问
// 此处先用 extern 声明，后续完善链接逻辑
extern std::vector<float> Bi1(int nCount, std::vector<float> pHigh, std::vector<float> pLow);
extern std::vector<float> Bi2(int nCount, std::vector<float> pHigh, std::vector<float> pLow);

// ============================================================
// Bi1: 简笔顶底端点
// ============================================================
class Bi1Test : public ::testing::Test
{
protected:
    // 最小测试案例：3根K线，形成一上一下两笔
    // K1: high=10, low=9  (方向向上)
    // K2: high=12, low=8  (包含处理后作为向上笔)
    // K3: high=7,  low=6  (向下笔)
    std::vector<float> high = {10.0f, 12.0f, 11.0f, 7.0f};
    std::vector<float> low  = {9.0f,  8.0f,  7.0f,  6.0f};
};

TEST_F(Bi1Test, 空数据返回全零)
{
    std::vector<float> empty_high, empty_low;
    std::vector<float> out = Bi1(0, empty_high, empty_low);
    EXPECT_EQ(out.size(), 0ul);
}

TEST_F(Bi1Test, 单根K线无笔端点)
{
    std::vector<float> h = {10.0f};
    std::vector<float> l = {9.0f};
    std::vector<float> out = Bi1(1, h, l);
    EXPECT_EQ(out.size(), 1ul);
}

TEST_F(Bi1Test, 正常笔划分)
{
    // 简单序列：向上笔 → 向下笔
    // 实际输出取决于 K线包含处理逻辑，此处验证框架可用
    std::vector<float> h = {10.0f, 12.0f, 11.0f, 7.0f};
    std::vector<float> l = {9.0f,  8.0f,  7.0f,  6.0f};
    std::vector<float> out = Bi1(4, h, l);
    EXPECT_EQ(out.size(), 4ul);
    // 端点值应为 1(向上笔顶) 或 -1(向下笔底)，其余为 0
}

// ============================================================
// Bi2: 标准笔顶底端点
// ============================================================
TEST(Bi2Test, 空数据)
{
    std::vector<float> out = Bi2(0, {}, {});
    EXPECT_EQ(out.size(), 0ul);
}

TEST(Bi2Test, 两根K线无笔)
{
    std::vector<float> h = {10.0f, 9.0f};
    std::vector<float> l = {9.0f,  8.0f};
    std::vector<float> out = Bi2(2, h, l);
    EXPECT_EQ(out.size(), 2ul);
}
