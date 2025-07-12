# naive method

Perhaps the simplest approach to performing in-database connected components analysis is to begin by choosing for each vertex a representative by picking the vertex with the minimum ID among the vertex itself and all its neighbours, then to improve on that representative by taking the minimum ID among the *representatives* of the vertex itself and all its neighbours, and to continue in this fashion until no vertex changes its choice of representative. We refer to this naive approach as the “Breadth First Search” strategy: after n steps each vertex’s representative is the vertex with the minimum ID among all vertices in the connected component that are at most at distance n from the original vertex. Though the algorithm ultimately terminates and delivers the correct result, its worst-case runtime makes it unsuitable for Big Data. Consider, for example, the sequentially numbered path graph with IDs 1, 2, . . . , n. For this graph, Breadth First Search will take n − 1 steps.

# paper method

好的！我们来构造一个稍微复杂一点的例子，图中包含多个连通分量和多个层级的“传播”，从而更清楚地展示 **Randomised Contraction（随机收缩）** 是如何逐轮缩小图并找到连通分量的。

------

## 🧠 示例图 G₀（初始图）

```
lua


复制编辑
连通分量1：      连通分量2：

1---2---3        6---7
 \     |         |
  \    |         8
   \   |
     4---5

单独节点： 9
```

### 🔢 顶点 V₀：

```
复制编辑
{1,2,3,4,5,6,7,8,9}
```

### 🔗 边 E₀：

```
scss


复制编辑
(1,2), (1,4), (2,3), (3,4), (4,5)
(6,7), (6,8)
```

共 9 个点，分为 3 个连通分量：

- {1,2,3,4,5}
- {6,7,8}
- {9}（孤立节点）

------

## 🔁 第 1 步：为每个点选择代表（闭邻域中最小 ID）

| 顶点 | 邻居  | 闭邻域  | 代表（最小 ID） |
| ---- | ----- | ------- | --------------- |
| 1    | 2,4   | 1,2,4   | 1               |
| 2    | 1,3   | 1,2,3   | 1               |
| 3    | 2,4   | 2,3,4   | 2               |
| 4    | 1,3,5 | 1,3,4,5 | 1               |
| 5    | 4     | 4,5     | 4               |
| 6    | 7,8   | 6,7,8   | 6               |
| 7    | 6     | 6,7     | 6               |
| 8    | 6     | 6,8     | 6               |
| 9    | —     | 9       | 9               |



------

## 🔁 构造收缩图 G₁

我们根据上面的代表构造新图：

### 顶点集合 V₁：

```
复制编辑
{1,2,4,6,9}
```

### 边集合 E₁：

遍历原图每条边 `(v, w)`，如果 `r(v) ≠ r(w)`，就保留一条 `(r(v), r(w))`：

- (1,2) → (1,1) → 忽略
- (1,4) → (1,1) → 忽略
- (2,3) → (1,2) → ✅ 保留 (1,2)
- (3,4) → (2,1) → ✅ 保留 (2,1)
- (4,5) → (1,4) → ✅ 保留 (1,4)
- (6,7), (6,8), (7,6) → 都是 (6,6) → 忽略
- 9 是孤立 → 保留

最终：

```
mathematica


复制编辑
E₁ = {(1,2), (1,4), (2,1)} （等价于 1–2, 1–4）
```

图 G₁：

```
lua


复制编辑
1---2
|
4

6   9 （孤立）
```

------

## 🔁 第 2 步：再次收缩

再次选代表：

| 顶点 | 邻居 | 闭邻域 | 代表 |
| ---- | ---- | ------ | ---- |
| 1    | 2,4  | 1,2,4  | 1    |
| 2    | 1    | 1,2    | 1    |
| 4    | 1    | 1,4    | 1    |
| 6    | —    | 6      | 6    |
| 9    | —    | 9      | 9    |



------

## 🔁 构造收缩图 G₂

- 所有 {1,2,4} 都指向 1 → 合并
- 没有新边了，只剩孤立点

最终 G₂ 的顶点是：

```
复制编辑
{1,6,9}（均为孤立点）
```

------

## ✅ 最终结果：每个顶点归属的连通分量 ID

从 r₁ 和 r₂ 合并的映射：

| 顶点 | r₁   | r₂(r₁) | 最终分量 ID |
| ---- | ---- | ------ | ----------- |
| 1    | 1    | 1      | 1           |
| 2    | 1    | 1      | 1           |
| 3    | 2    | 1      | 1           |
| 4    | 1    | 1      | 1           |
| 5    | 4    | 1      | 1           |
| 6    | 6    | 6      | 6           |
| 7    | 6    | 6      | 6           |
| 8    | 6    | 6      | 6           |
| 9    | 9    | 9      | 9           |



------

## 🎯 输出：

每个顶点对应的连通分量 ID：

```
diff


复制编辑
+-------+---------------------+
| 顶点  | 连通分量 ID (label) |
+-------+---------------------+
| 1     | 1                   |
| 2     | 1                   |
| 3     | 1                   |
| 4     | 1                   |
| 5     | 1                   |
| 6     | 6                   |
| 7     | 6                   |
| 8     | 6                   |
| 9     | 9                   |
+-------+---------------------+
```

------

如果你想，我可以用 SQL 或图图像将这个过程可视化，或者给出这个算法的递归表达式实现。是否需要？