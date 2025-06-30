Input: source node s, target node t  
Output: shortest path between s and t  

// 0) Initialization  
INSERT INTO TAf(nid, p2s, d2s, f)  
VALUES(s, s, 0, 0);       # seed forward table  
INSERT INTO TAb(nid, p2s, d2t, b)  
VALUES(t, t, 0, 0);       # seed backward table  

// 1) Main loop: alternate forward/backward expansions  
WHILE (lf + lb < min_cost) AND (nf > 0) AND (nb > 0) DO  

    // mark next frontier and build view er  
    PERFORM build_forward_er_view();  

    // merge view er into TAf, return affected rows  
    nf ← merge_forward();  

    // update smallest unfinalized forward distance  
    lf ← SELECT MIN(d2s) FROM TAf WHERE f = 0;  

    // mark next frontier and build view er  
    PERFORM build_backward_er_view();  

    // merge view er into TAb, return affected rows  
    nb ← merge_backward();  

    // update smallest unfinalized backward distance  
    lb ← SELECT MIN(d2t) FROM TAb WHERE b = 0;  

    // update current best s→t distance  
    min_cost ← SELECT COALESCE(MIN(d2s + d2t), ∞)  
               FROM TAf JOIN TAb USING(nid);  

END WHILE  

// 2) Recover the meeting node where the two searches met  
xid ← SELECT nid  
       FROM TAf JOIN TAb USING(nid)  
       WHERE d2s + d2t = min_cost;  

RETURN xid  
