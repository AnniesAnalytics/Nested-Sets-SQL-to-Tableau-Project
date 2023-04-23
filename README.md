# Nested-Sets-SQL-to-Tableau-Project
I worked with a client who had a nested sets database structure. I wanted to create a sunburst for one of their dashboards, but I needed the data to be in a very specific form for that. Here's the custom SQL I wrote in Tableau to automate that from their database.

I de-idenfitifed this by changing the names of things in the query to reflect a supermarket heirarchy, my original client was a different use case.


-- First CTE: tiered_food_departments
-- This CTE groups food departments into their respective tiers and associates them with a unique ID and full name.
WITH tiered_food_departments AS (
    SELECT
        fd.id,
        fd.full_name,
        (array_agg(tiers.name))[1] as tier_1,
        (array_agg(tiers.name))[2] as tier_2,
        (array_agg(tiers.name))[3] as tier_3,
        (array_agg(tiers.name))[4] as tier_4
    FROM food_departments fd
    JOIN LATERAL (
        -- This subquery selects and orders the tiers of food_departments.
        SELECT name, row_number() OVER (ORDER BY t1.left) tier
        FROM food_departments t1
        WHERE t1.id != 1 AND t1.left <= fd.left AND t1.right >= fd.right
        ORDER BY t1.left
    ) AS tiers ON true
    -- Only leaf-nodes are considered for further processing.
    WHERE fd.left = fd.right - 1
    GROUP BY fd.id
    ORDER BY tier_1, tier_2, tier_3, tier_4
),
-- Second CTE: nodes_hierarchy
-- This CTE is used to create a hierarchy of nodes in the food departments, including their root, leaf, and parent nodes.
nodes_hierarchy AS (
    -- First part of the UNION ALL: Selecting the root nodes (Tier 1)
    SELECT
        id,
        full_name,
        tier_1 AS "Root",
        COALESCE(tier_4 || ' - ' || tier_3, tier_3 || ' - ' || tier_2, tier_2 || ' - ' || tier_1, tier_1) AS "Leaf",
        COALESCE(tier_4, tier_3, tier_2, tier_1) AS "Join",
        1 AS "Level",
        tier_1 AS "All",
        NULL AS "Parent",
        NULL AS "Parent Join",
        tier_1,
        NULL AS tier_2,
        NULL AS tier_3,
        NULL AS tier_4
    FROM tiered_food_departments
    UNION ALL
    -- Second part of the UNION ALL: Selecting the nodes in Tier 2
    SELECT
        id,
        full_name,
        tier_1 AS "Root",
        COALESCE(tier_4 || ' - ' || tier_3, tier_3 || ' - ' || tier_2, tier_2 || ' - ' || tier_1, tier_1) AS "Leaf",
        COALESCE(tier_4, tier_3, tier_2, tier_1) AS "Join",
        2 AS "Level",
        tier_2 || ' - ' || tier_1 AS "All",
        tier_1 AS "Parent",
        tier_1 AS "Parent Join",
        tier_1,
        tier_2 || ' - ' || tier_1 AS tier_2,
        NULL AS tier_3,
        NULL AS tier_4
    FROM tiered_food_departments
    WHERE tier_2 IS NOT NULL
    UNION ALL
    -- Third part of the UNION ALL: Selecting the nodes in Tier 3
         SELECT
            id,
            full_name,
            tier_1 AS "Root",
            COALESCE(tier_4 || ' - ' || tier_3, tier_3 || ' - ' || tier_2, tier_2 || ' - ' || tier_1, tier_1) AS "Leaf",
            COALESCE(tier_4, tier_3, tier_2, tier_1) AS "Join",
            3 AS "Level",
            tier_3 || ' - ' || tier_2 AS "All",
            tier_2 || ' - ' || tier_1 AS "Parent",
            tier_2 AS "Parent Join",
            tier_1,
            tier_2 || ' - ' || tier_1 AS tier_2,
            tier_3 || ' - ' || tier_2 AS tier_3,
            NULL AS tier_4
        FROM tiered_food_departments
        WHERE tier_3 IS NOT NULL
        UNION ALL
        -- Fourth part of the UNION ALL: Selecting the nodes in Tier 4
        SELECT
            id,
            full_name,
            tier_1 AS "Root",
            COALESCE(tier_4 || ' - ' || tier_3, tier_3 || ' - ' || tier_2, tier_2 || ' - ' || tier_1, tier_1) AS "Leaf",
            COALESCE(tier_4, tier_3, tier_2, tier_1) AS "Join",
            4 AS "Level",
            tier_4 || ' - ' || tier_3 AS "All",
            tier_3 || ' - ' || tier_2 AS "Parent",
            tier_3 AS "Parent Join",
            tier_1,
            tier_2 || ' - ' || tier_1 AS tier_2,
            tier_3 || ' - ' || tier_2 AS tier_3,
            tier_4 || ' - ' || tier_3 AS tier_4
        FROM tiered_food_departments
        WHERE tier_4 IS NOT NULL
    ),

    -- Third CTE: nodes_hierarchy_with_sort
    -- This CTE sorts the nodes in the hierarchy using DENSE_RANK().
    nodes_hierarchy_with_sort AS (
        SELECT *,
            DENSE_RANK() OVER (ORDER BY "Level", "Root", "Parent" NULLS FIRST, "All") AS "Sort"
        FROM nodes_hierarchy
    )

SELECT
    "All",
    "Level",
    "Parent",
    "Parent Join",
    "Root",
    "Leaf",
    "Join",
    full_name,
    "Sort",
    tier_1 AS "Tier 1",
    tier_2 AS "Tier 2",
    tier_3 AS "Tier 3",
    tier_4 AS "Tier 4"
FROM nodes_hierarchy_with_sort
ORDER BY "Sort", "Level"
