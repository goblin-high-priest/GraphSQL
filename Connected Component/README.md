# naive method

Perhaps the simplest approach to performing in-database connected components analysis is to begin by choosing for each vertex a representative by picking the vertex with the minimum ID among the vertex itself and all its neighbours, then to improve on that representative by taking the minimum ID among the *representatives* of the vertex itself and all its neighbours, and to continue in this fashion until no vertex changes its choice of representative. We refer to this naive approach as the “Breadth First Search” strategy: after n steps each vertex’s representative is the vertex with the minimum ID among all vertices in the connected component that are at most at distance n from the original vertex. Though the algorithm ultimately terminates and delivers the correct result, its worst-case runtime makes it unsuitable for Big Data. Consider, for example, the sequentially numbered path graph with IDs 1, 2, . . . , n. For this graph, Breadth First Search will take n − 1 steps.

# paper method (without randomised)

## Example Graph G₀ (Initial Graph)

```

Connected Component 1:      Connected Component 2:

1---2---3                   6---7
 \     |                    |
  \    |                    8
   \   |
     4---5

Isolated Node: 9
```

### Vertex Set V₀:

```
{1, 2, 3, 4, 5, 6, 7, 8, 9}
```

### Edge Set E₀:

```

(1,2), (1,4), (2,3), (3,4), (4,5)
(6,7), (6,8)
```

There are 9 nodes in total, forming 3 connected components:

- {1,2,3,4,5}
- {6,7,8}
- {9} (isolated node)

------

## Step 1: Assign representatives (minimum ID in closed neighborhood)

| Node | Neighbors | Closed Neighborhood | Representative |
| ---- | --------- | ------------------- | -------------- |
| 1    | 2, 4      | 1, 2, 4             | 1              |
| 2    | 1, 3      | 1, 2, 3             | 1              |
| 3    | 2, 4      | 2, 3, 4             | 2              |
| 4    | 1, 3, 5   | 1, 3, 4, 5          | 1              |
| 5    | 4         | 4, 5                | 4              |
| 6    | 7, 8      | 6, 7, 8             | 6              |
| 7    | 6         | 6, 7                | 6              |
| 8    | 6         | 6, 8                | 6              |
| 9    | —         | 9                   | 9              |



------

## Build Contracted Graph G₁

For each edge (v, w), retain (r(v), r(w)) only if r(v) ≠ r(w):

- (1,2) → (1,1) → dropped
- (1,4) → (1,1) → dropped
- (2,3) → (1,2) → kept
- (3,4) → (2,1) → kept
- (4,5) → (1,4) → kept
- (6,7), (6,8), (7,6) → all become (6,6) → dropped

Final edge set:

```
(1,2), (2,1), (1,4)
```

Graph G₁ becomes:

```

1---2
|
4

6   9
```

------

## Step 2: Assign representatives again

| Node | Neighbors | Closed Neighborhood | Representative |
| ---- | --------- | ------------------- | -------------- |
| 1    | 2, 4      | 1, 2, 4             | 1              |
| 2    | 1         | 1, 2                | 1              |
| 4    | 1         | 1, 4                | 1              |
| 6    | —         | 6                   | 6              |
| 9    | —         | 9                   | 9              |



------

## Build Contracted Graph G₂

All nodes {1,2,4} are mapped to 1 → merged
 No more new edges remain. Only isolated nodes left.

Vertex set of G₂:

```
{1, 6, 9}
```

------

## Final Result: Connected Component Labels for Each Node

Backtracking through r₁ and r₂ gives the final mapping:

| Node | r₁   | r₂(r₁) | Final Label |
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

## Output

```
diff


复制编辑
+-------+------------------+
| Node  | Connected Label  |
+-------+------------------+
| 1     | 1                |
| 2     | 1                |
| 3     | 1                |
| 4     | 1                |
| 5     | 1                |
| 6     | 6                |
| 7     | 6                |
| 8     | 6                |
| 9     | 9                |
+-------+------------------+
```
