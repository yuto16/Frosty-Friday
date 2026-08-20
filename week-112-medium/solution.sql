/************************************************************************************************
 * Frosty Friday Week 112: Geospatial (Medium)
 *
 * 【概要】
 * Snowflake World Tour の開催都市（18都市／Americas・EMEA・APJ）から、
 * リージョンごとに「開催日順」でポリゴンを組み立てる、という課題です。
 *
 * 【この解法の主題】
 * 実は「開催日順にそのまま結ぶ」と 3リージョンすべてでポリゴンを作れません。
 * 環が自己交差するためです。この失敗こそが本課題の学びどころなので、
 *   STEP 2 でわざと失敗させ → STEP 3 で原因を突き止め → STEP 4〜6 で 2通りの解法を比較
 * という順に進めます。
 *
 * 【注意】
 * STEP 2 の最後の 1文は「意図的にエラーになる」文です。
 * Snowsight で 1文ずつ実行してください。ファイル一括実行だとそこで停止します。
 *
 * 参考: https://www.frostyfri.day/en/challenges/blog/2024/09/27/week-112-geospatial
 * 公式ドキュメント: https://docs.snowflake.com/en/sql-reference/functions-geospatial
 *                  https://docs.snowflake.com/en/sql-reference/data-types-geospatial
 ************************************************************************************************/

-----------------------------------------------------------------------
-- STEP 0: 環境準備
-----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 既存の他 week のスキーマを壊さないよう、DB は IF NOT EXISTS で作る。
-- （CREATE OR REPLACE DATABASE にすると DB ごと作り直され、他 week が消えます）
CREATE DATABASE IF NOT EXISTS FROSTYFRIDAY;
CREATE OR REPLACE SCHEMA FROSTYFRIDAY.WEEK112;
USE SCHEMA FROSTYFRIDAY.WEEK112;


-----------------------------------------------------------------------
-- STEP 1: スタートコードの投入とデータ確認
-----------------------------------------------------------------------

-- ▼ここからお題提供のスタートコード（原文ママ）
CREATE OR REPLACE TABLE event_schedule_geo (
    Region VARCHAR,
    City VARCHAR,
    Event_Date DATE,
    GPS_Coordinates VARCHAR,
    Geo_Point GEOGRAPHY
);

INSERT INTO event_schedule_geo (Region, City, Event_Date, Geo_Point)
SELECT
    'Americas', 'Atlanta', '2024-10-03', TO_GEOGRAPHY('POINT(-84.3880 33.7490)')
UNION ALL SELECT
    'Americas', 'Bogotá', '2024-10-30', TO_GEOGRAPHY('POINT(-74.0721 4.7110)')
UNION ALL SELECT
    'Americas', 'Chicago', '2024-11-04', TO_GEOGRAPHY('POINT(-87.6298 41.8781)')
UNION ALL SELECT
    'Americas', 'Dallas', '2024-10-01', TO_GEOGRAPHY('POINT(-96.7970 32.7767)')
UNION ALL SELECT
    'Americas', 'Mexico City', '2024-10-24', TO_GEOGRAPHY('POINT(-99.1332 19.4326)')
UNION ALL SELECT
    'Americas', 'New York City', '2024-10-15', TO_GEOGRAPHY('POINT(-74.0060 40.7128)')
UNION ALL SELECT
    'Americas', 'São Paulo', '2024-10-08', TO_GEOGRAPHY('POINT(-46.6333 -23.5505)')
UNION ALL SELECT
    'Americas', 'Toronto', '2024-10-21', TO_GEOGRAPHY('POINT(-79.347015 43.651070)')
UNION ALL SELECT
    'EMEA', 'Amsterdam', '2024-10-03', TO_GEOGRAPHY('POINT(4.9041 52.3676)')
UNION ALL SELECT
    'EMEA', 'Berlin', '2024-10-16', TO_GEOGRAPHY('POINT(13.4050 52.5200)')
UNION ALL SELECT
    'EMEA', 'London', '2024-10-10', TO_GEOGRAPHY('POINT(-0.1278 51.5074)')
UNION ALL SELECT
    'EMEA', 'Paris', '2024-10-01', TO_GEOGRAPHY('POINT(2.3522 48.8566)')
UNION ALL SELECT
    'EMEA', 'Stockholm', '2024-10-17', TO_GEOGRAPHY('POINT(18.0686 59.3293)')
UNION ALL SELECT
    'APJ', 'Kuala Lumpur', '2024-10-23', TO_GEOGRAPHY('POINT(101.6869 3.1390)')
UNION ALL SELECT
    'APJ', 'Mumbai', '2024-10-04', TO_GEOGRAPHY('POINT(72.8777 19.0760)')
UNION ALL SELECT
    'APJ', 'Auckland', '2024-10-24', TO_GEOGRAPHY('POINT(174.7633 -36.8485)')
UNION ALL SELECT
    'APJ', 'Manila', '2024-10-02', TO_GEOGRAPHY('POINT(120.9842 14.5995)')
UNION ALL SELECT
    'APJ', 'Sydney', '2024-10-29', TO_GEOGRAPHY('POINT(151.2093 -33.8688)');
-- ▲ここまでスタートコード

/* 【学び①】スタートコードに仕込まれた罠
   INSERT の列リストは (Region, City, Event_Date, Geo_Point) で、GPS_Coordinates が入っていません。
   つまり VARCHAR 側の座標カラムは全行 NULL です。「あるはずのカラムが空」は実務でも頻出なので、
   COUNT(*) と COUNT(列) の差で必ず確認する癖をつけます。
   （COUNT(*) は行数、COUNT(列) は NULL を除いた件数） */
SELECT
    COUNT(*)               AS rows_total,     -- 期待値: 18
    COUNT(GPS_Coordinates) AS gps_not_null,   -- 期待値: 0  ← 全行 NULL
    COUNT(Geo_Point)       AS geo_not_null    -- 期待値: 18
FROM event_schedule_geo;

/* 【学び②】GEOGRAPHY の表示形式はセッションパラメータで変わる
   既定は GeoJSON。人が読む・WKT を組み立てる用途では WKT のほうが扱いやすいです。 */
ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'WKT';

-- ST_X / ST_Y で経度・緯度を取り出せる（WKT は「経度 緯度」の順。緯度経度の逆順に注意）
SELECT
    region,
    city,
    event_date,
    ST_ASWKT(geo_point) AS wkt,
    ST_X(geo_point)     AS lon,
    ST_Y(geo_point)     AS lat
FROM event_schedule_geo
ORDER BY region, event_date;

-- リージョンごとの都市数と開催期間
-- 期待値: APJ 5都市 / Americas 8都市 / EMEA 5都市
SELECT region, COUNT(*) AS cities, MIN(event_date) AS first_event, MAX(event_date) AS last_event
FROM event_schedule_geo
GROUP BY region
ORDER BY region;


-----------------------------------------------------------------------
-- STEP 2: 素直な挑戦 ―― 開催日順に頂点を並べてポリゴンを作る
--         （このSTEPは意図的に失敗します）
-----------------------------------------------------------------------

/* 【学び③】ST_COLLECT は順序を保証しない
   「点をまとめる」と聞くと ST_COLLECT を使いたくなりますが、これは MULTIPOINT を返す集約関数で、
   並び順に意味を持たせられません。ポリゴンの頂点列のように「順序が本質」の集約では、
   ARRAY_AGG(...) WITHIN GROUP (ORDER BY ...) を使って明示的に順序を与えます。 */
SELECT ST_ASWKT(ST_COLLECT(geo_point)) AS collected   -- MULTIPOINT が返る（頂点の順序は不定）
FROM event_schedule_geo
WHERE region = 'EMEA';

/* ポリゴンの WKT を組み立てる。
   ポイントは2つ:
     1. WITHIN GROUP (ORDER BY event_date) で開催日順に並べる
     2. ARRAY_APPEND(pts, pts[0]) で先頭の点を末尾にもう一度足し、環を閉じる
        （WKT のポリゴンは始点と終点が一致していないと受け付けられません） */
WITH ordered AS (
    SELECT
        region,
        ARRAY_AGG(ST_X(geo_point) || ' ' || ST_Y(geo_point))
            WITHIN GROUP (ORDER BY event_date) AS pts
    FROM event_schedule_geo
    GROUP BY region
)
SELECT
    region,
    ARRAY_SIZE(pts) AS n_cities,
    'POLYGON((' || ARRAY_TO_STRING(ARRAY_APPEND(pts, pts[0]::STRING), ', ') || '))' AS ring_wkt
FROM ordered
ORDER BY region;

/* 期待値（実行結果）:
   APJ      | 5 | POLYGON((120.9842 14.5995, 72.8777 19.076, 101.6869 3.139, 174.7633 -36.8485, 151.2093 -33.8688, 120.9842 14.5995))
   Americas | 8 | POLYGON((-96.797 32.7767, -84.388 33.749, -46.6333 -23.5505, -74.006 40.7128, -79.347015 43.65107, -99.1332 19.4326, -74.0721 4.711, -87.6298 41.8781, -96.797 32.7767))
   EMEA     | 5 | POLYGON((2.3522 48.8566, 4.9041 52.3676, -0.1278 51.5074, 13.405 52.52, 18.0686 59.3293, 2.3522 48.8566))
   文字列としては正しい WKT に見えます。では GEOGRAPHY に変換してみます。 */

-- ★ここが意図的に失敗する1文（上で出力された EMEA の WKT をそのまま渡しています）
-- 期待値: 100217 (P0000): Geography validation failed:
--         Edge (2.352200,48.856600) -- (4.904100,52.367600) crosses edge (-0.127800,51.507400) -- (13.405000,52.520000)
SELECT TO_GEOGRAPHY('POLYGON((2.3522 48.8566, 4.9041 52.3676, -0.1278 51.5074, 13.405 52.52, 18.0686 59.3293, 2.3522 48.8566))');

/* 何が起きたか:
   Paris(10/01) → Amsterdam(10/03) → London(10/10) → Berlin(10/16) → Stockholm(10/17) → Paris
   と結ぶと、Paris→Amsterdam の辺と London→Berlin の辺が交差します。
   開催日は地理的な並びと無関係なので、日付順に結べば当然こうなります。 */


-----------------------------------------------------------------------
-- STEP 3: 原因究明 ―― なぜ作れないのかを SQL で確かめる
-----------------------------------------------------------------------

/* 【学び④】TO_* と TRY_TO_* の使い分け
   TO_GEOGRAPHY は失敗するとクエリごと落ちます。全リージョンをまとめて調べたいときは
   TRY_TO_GEOGRAPHY を使うと、失敗を NULL として受け取れて一覧化できます。 */
WITH ordered AS (
    SELECT
        region,
        ARRAY_AGG(ST_X(geo_point) || ' ' || ST_Y(geo_point))
            WITHIN GROUP (ORDER BY event_date) AS pts
    FROM event_schedule_geo
    GROUP BY region
),
wkt AS (
    SELECT
        region,
        'POLYGON((' || ARRAY_TO_STRING(ARRAY_APPEND(pts, pts[0]::STRING), ', ') || '))' AS ring_wkt,
        'LINESTRING(' || ARRAY_TO_STRING(pts, ', ') || ')'                              AS route_wkt
    FROM ordered
)
SELECT
    region,
    TRY_TO_GEOGRAPHY(ring_wkt)  IS NULL     AS polygon_rejected,   -- 期待値: 3リージョンとも True
    TRY_TO_GEOGRAPHY(route_wkt) IS NOT NULL AS linestring_accepted -- 期待値: 3リージョンとも True
FROM wkt
ORDER BY region;

/* 【学び⑤】ポリゴンだけが弾かれ、同じ頂点列の LINESTRING は通る
   LINESTRING に自己交差の制約はありません。制約があるのはポリゴン（単純多角形であること）です。
   つまり問題は「座標が変」でも「球面だから」でもなく、位相（トポロジー）の要件違反です。 */

/* 【学び⑥】GEOMETRY に逃げても解決しない
   GEOGRAPHY（球面）ではなく GEOMETRY（平面）なら通るのでは？と考えたくなりますが、同じく弾かれます。
   期待値: 100383 (P0000): Geometry validation failed:
           Geometry has invalid self-intersections. A self-intersection point was found at (7.14717, 52.0518)
   ※この1文も意図的に失敗します。確認したら次へ進んでください。 */
-- SELECT TO_GEOMETRY('POLYGON((2.3522 48.8566, 4.9041 52.3676, -0.1278 51.5074, 13.405 52.52, 18.0686 59.3293, 2.3522 48.8566))');

/* 【学び⑦】Snowflake は「型に入れる時点で」妥当性を検証する
   PostGIS のように「不正なジオメトリを格納しておいて、後から ST_ISVALID で調べる」ことはできません。
   TO_GEOGRAPHY / TO_GEOMETRY を通った時点で妥当性は保証されている、という設計です。
   よって対策は「作ってから直す」ではなく「作る前に頂点順を決める」になります。 */


-----------------------------------------------------------------------
-- STEP 4: 解法A ―― 重心まわりの角度ソートで単純多角形にする
-----------------------------------------------------------------------

/* 考え方:
   リージョンの重心を求め、各都市が重心から見てどの方角にあるか（方位角）を ATAN2 で計算し、
   その角度順に頂点を並べます。重心のまわりを一周する順序になるので、辺は交差しません。

   ATAN2(y, x) は引数の順が (y, x) である点に注意（緯度差, 経度差 の順）。
   -π〜π を返すので、そのまま ORDER BY すれば反時計回りに並びます。

   ※ 凸包（convex hull）ではありません。Snowflake には ST_CONVEXHULL が存在しないため
     （実行すると "Unknown function ST_CONVEXHULL"）、この角度ソートが実用的な代替になります。
     凹んだ配置では厳密な凸包と一致しませんが、全都市を頂点として保持できる利点があります。 */
CREATE OR REPLACE VIEW region_polygon_angular AS
WITH centroid AS (
    -- リージョンごとの重心（経度・緯度の単純平均）
    SELECT region, AVG(ST_X(geo_point)) AS c_lon, AVG(ST_Y(geo_point)) AS c_lat
    FROM event_schedule_geo
    GROUP BY region
),
angled AS (
    -- 重心から見た各都市の方位角
    SELECT
        e.region,
        ST_X(e.geo_point) AS lon,
        ST_Y(e.geo_point) AS lat,
        ATAN2(ST_Y(e.geo_point) - c.c_lat, ST_X(e.geo_point) - c.c_lon) AS bearing
    FROM event_schedule_geo e
    JOIN centroid c ON e.region = c.region
),
ring AS (
    -- 方位角順に並べ、始点を末尾に足して環を閉じる
    SELECT region, ARRAY_AGG(lon || ' ' || lat) WITHIN GROUP (ORDER BY bearing) AS pts
    FROM angled
    GROUP BY region
)
SELECT
    region,
    TO_GEOGRAPHY('POLYGON((' || ARRAY_TO_STRING(ARRAY_APPEND(pts, pts[0]::STRING), ', ') || '))') AS region_polygon
FROM ring;

-- 期待値: 3行とも TO_GEOGRAPHY を通過し、ポリゴンが返る
--   APJ      | 6 頂点(閉点含む) | 17,766,431 km2
--   Americas | 9 頂点(閉点含む) | 13,709,511 km2
--   EMEA     | 6 頂点(閉点含む) |    446,345 km2
SELECT
    region,
    ST_NPOINTS(region_polygon)             AS n_points,   -- 都市数 +1（環を閉じる点の分）
    ROUND(ST_AREA(region_polygon) / 1e6)   AS area_km2,   -- ST_AREA は平方メートルで返る
    ST_ASWKT(region_polygon)               AS wkt
FROM region_polygon_angular
ORDER BY region;

/* 別解: WKT 文字列で 'POLYGON((...))' を組み立てる代わりに、閉じた LINESTRING を
   ST_MAKEPOLYGON に渡す書き方。ST_MAKEPOLYGON は「外周の LineString からポリゴンを作る」関数で、
   渡す LineString は始点と終点が一致している必要があります。
   下の頂点順は、上の VIEW の方位角ソートが EMEA について与える順序
   （London→Amsterdam→Paris→Berlin→Stockholm）を書き下したものです。
   期待値: 上の EMEA 行と同じ POLYGON / 446,345 km2 が返る */
SELECT
    ST_ASWKT(ST_MAKEPOLYGON(
        TO_GEOGRAPHY('LINESTRING(-0.1278 51.5074, 4.9041 52.3676, 2.3522 48.8566, 13.405 52.52, 18.0686 59.3293, -0.1278 51.5074)')
    )) AS emea_polygon_via_makepolygon,
    ROUND(ST_AREA(ST_MAKEPOLYGON(
        TO_GEOGRAPHY('LINESTRING(-0.1278 51.5074, 4.9041 52.3676, 2.3522 48.8566, 13.405 52.52, 18.0686 59.3293, -0.1278 51.5074)')
    )) / 1e6) AS area_km2;


-----------------------------------------------------------------------
-- STEP 5: 解法B ―― ST_ENVELOPE で外接矩形を作る
-----------------------------------------------------------------------

/* 最短の解。ST_COLLECT で MULTIPOINT にまとめ、ST_ENVELOPE でその外接矩形を得ます。
   頂点の順序を一切考えなくてよいので、常に成功します。 */
CREATE OR REPLACE VIEW region_polygon_envelope AS
SELECT region, ST_ENVELOPE(ST_COLLECT(geo_point)) AS region_polygon
FROM event_schedule_geo
GROUP BY region;

-- 期待値: 3行とも 5頂点(矩形4隅＋閉点)
--   APJ 82,750,629 km2 / Americas 42,428,026 km2 / EMEA 1,368,781 km2
SELECT
    region,
    ST_NPOINTS(region_polygon)           AS n_points,
    ROUND(ST_AREA(region_polygon) / 1e6) AS area_km2,
    ST_ASWKT(region_polygon)             AS wkt
FROM region_polygon_envelope
ORDER BY region;


-----------------------------------------------------------------------
-- STEP 6: 解法A と 解法B の比較 ―― どちらが「正解」か
-----------------------------------------------------------------------

/* 面積で比べると差は歴然です。fill_ratio は「外接矩形に対して角度ソート多角形が占める割合」。
   期待値:
     APJ      | 17,766,431 / 82,750,629 = 0.215
     Americas | 13,709,511 / 42,428,026 = 0.323
     EMEA     |    446,345 /  1,368,781 = 0.326
   外接矩形はリージョンの2〜5倍の面積を主張してしまう、ということです。 */
SELECT
    a.region,
    ST_NPOINTS(a.region_polygon)                                AS pts_angular,
    ROUND(ST_AREA(a.region_polygon) / 1e6)                      AS area_angular_km2,
    ST_NPOINTS(v.region_polygon)                                AS pts_envelope,
    ROUND(ST_AREA(v.region_polygon) / 1e6)                      AS area_envelope_km2,
    ROUND(ST_AREA(a.region_polygon) / ST_AREA(v.region_polygon), 3) AS fill_ratio,
    ST_ASWKT(ST_CENTROID(a.region_polygon))                     AS centroid_angular
FROM region_polygon_angular  a
JOIN region_polygon_envelope v ON a.region = v.region
ORDER BY a.region;

/* 【学び⑧】ST_WITHIN は境界上の点を「含まない」
   「全都市が自リージョンのポリゴンに入っているか」を ST_WITHIN で調べると、
   角度ソート多角形では 18都市すべてが False になります。都市がそのまま多角形の頂点＝境界上だからです。
   境界を含めたいときは ST_COVERS（または ST_COVEREDBY）を使います。この差は実務で必ず踏みます。

   期待値:
     APJ      | 5 | within_angular 0 | covers_angular 5 | within_envelope 3 | covers_envelope 5
     Americas | 8 | within_angular 0 | covers_angular 8 | within_envelope 6 | covers_envelope 8
     EMEA     | 5 | within_angular 0 | covers_angular 5 | within_envelope 2 | covers_envelope 4  ← 4! */
SELECT
    e.region,
    COUNT(*)                                                  AS cities,
    SUM(IFF(ST_WITHIN(e.geo_point, a.region_polygon), 1, 0))  AS within_angular,
    SUM(IFF(ST_COVERS(a.region_polygon, e.geo_point), 1, 0))  AS covers_angular,
    SUM(IFF(ST_WITHIN(e.geo_point, v.region_polygon), 1, 0))  AS within_envelope,
    SUM(IFF(ST_COVERS(v.region_polygon, e.geo_point), 1, 0))  AS covers_envelope
FROM event_schedule_geo e
JOIN region_polygon_angular  a ON e.region = a.region
JOIN region_polygon_envelope v ON e.region = v.region
GROUP BY e.region
ORDER BY e.region;

/* 【学び⑨】外接矩形は、自分の入力点を含むとは限らない ―― 球面ジオメトリの核心
   上の covers_envelope が EMEA だけ 5 ではなく 4 です。つまり ST_ENVELOPE が作った矩形が、
   その矩形を作る元になった都市を1つ含んでいません。バグではありません。

   GEOGRAPHY の辺は「大円弧（2点間の最短経路）」です。緯度が一定の線（緯線）は、赤道以外では
   大円ではありません。同じ緯度の2点を結ぶ大円弧は、必ず極側へ膨らみます。
   矩形の南辺 lat=48.8566 を結ぶ大円弧は北へ膨らむので、同じ緯度 48.8566 にある Paris は
   その弧の南側＝矩形の外に落ちます。 */
SELECT
    e.city,
    ST_Y(e.geo_point)                              AS lat,
    ST_X(e.geo_point)                              AS lon,
    ST_ASWKT(v.region_polygon)                     AS envelope_wkt,
    ROUND(ST_DISTANCE(e.geo_point, v.region_polygon), 1) AS gap_m   -- 期待値: 18781.4 m（約19km 外側）
FROM event_schedule_geo e
JOIN region_polygon_envelope v ON e.region = v.region
WHERE NOT ST_COVERS(v.region_polygon, e.geo_point);
-- 期待値: Paris (48.8566, 2.3522) の1行だけが返る

/* 「たまたま Paris だけの偶然では？」と思うところなので、条件を特定しておきます。
   危ないのは「緯度の端にありながら、経度の端ではない都市」＝矩形の辺の途中に乗る都市です。
   逆に、緯度の端かつ経度の端なら矩形の"角"になるので、必ず矩形上に乗ります。

   下は各リージョンの最南の都市を調べるクエリ。期待値:
     APJ      | Auckland  | is_lon_extreme_too True  | covered True   ← 最南かつ最東＝角
     Americas | São Paulo | is_lon_extreme_too True  | covered True   ← 最南かつ最東＝角
     EMEA     | Paris     | is_lon_extreme_too False | covered False  ← 最南だが経度は中間＝辺の途中

   さらに「北半球なら南辺が危険、南半球なら北辺が危険」（＝赤道に近い側の辺が危険）です。
   弧は極側へ膨らむので、極側の辺では都市は弧の内側に入って助かります。
   Americas の最北 Toronto は経度の端ではありませんが、北半球の北辺なので covered = True です。 */
WITH extremes AS (
    SELECT
        region,
        MIN(ST_Y(geo_point)) AS min_lat,
        MIN(ST_X(geo_point)) AS min_lon,
        MAX(ST_X(geo_point)) AS max_lon
    FROM event_schedule_geo
    GROUP BY region
)
SELECT
    e.region,
    e.city,
    ST_X(e.geo_point) AS lon,
    ST_Y(e.geo_point) AS lat,
    (ST_X(e.geo_point) = x.min_lon OR ST_X(e.geo_point) = x.max_lon) AS is_lon_extreme_too,
    ST_COVERS(v.region_polygon, e.geo_point)                         AS covered
FROM event_schedule_geo e
JOIN extremes x               ON e.region = x.region AND ST_Y(e.geo_point) = x.min_lat
JOIN region_polygon_envelope v ON e.region = v.region
ORDER BY e.region;

/* 結論:
   - 解法A（角度ソート）: 全都市を頂点として保持し、面積も実態に近い。都市は境界上になる。
   - 解法B（外接矩形）  : 1行で書けて常に成功。ただし面積を大きく過大評価し、
                          さらに球面の性質上、入力点を含まないことすらある。
   どちらが正解かは用途次第です。「リージョンの広がりを表す図形」なら A、
   「粗い絞り込み用のバウンディングボックス」なら B、という使い分けになります。 */


-----------------------------------------------------------------------
-- STEP 7: 開催日順を捨てない ―― 巡回ルートを LINESTRING で表現する
-----------------------------------------------------------------------

/* ポリゴンには使えなかった開催日順ですが、LINESTRING（ツアーの巡回ルート）としては有効です。
   STEP 3 で確認したとおり、線には自己交差の制約がありません。
   「リージョンの形＝ポリゴン」「日程の流れ＝ルート」と2つの成果物に分けることで、
   設問の "organized by the event dates" を意味のある形で満たします。 */
CREATE OR REPLACE VIEW region_tour_route AS
WITH ordered AS (
    SELECT
        region,
        ARRAY_AGG(ST_X(geo_point) || ' ' || ST_Y(geo_point))
            WITHIN GROUP (ORDER BY event_date) AS pts
    FROM event_schedule_geo
    GROUP BY region
)
SELECT region, TO_GEOGRAPHY('LINESTRING(' || ARRAY_TO_STRING(pts, ', ') || ')') AS route
FROM ordered;

-- 期待値: APJ 5停留 19,609km / Americas 8停留 27,691km / EMEA 5停留 2,530km
SELECT
    region,
    ST_NPOINTS(route)              AS stops,
    ROUND(ST_LENGTH(route) / 1000) AS route_km,   -- ST_LENGTH もメートル
    ST_ASWKT(route)                AS route_wkt
FROM region_tour_route
ORDER BY region;

/* 【学び⑩】LAG は GEOGRAPHY を受け取れない
   区間距離を出そうと LAG(geo_point) と書くと、こうなります:
     001044 (42P13): SQL compilation error: Invalid argument types for function 'LAG': (GEOGRAPHY)
   ウィンドウ関数の多くは半構造化/地理空間型を直接扱えません。
   回避策は「スカラーに分解して LAG し、再構築する」こと。ここでは経度・緯度を別々に LAG して
   ST_MAKEPOINT で点に戻します。 */
-- SELECT LAG(geo_point) OVER (PARTITION BY region ORDER BY event_date) FROM event_schedule_geo;  -- ← エラーになる

-- 期待値: Paris NULL（各リージョンの初日は前の都市が無いので NULL）
--         / Amsterdam 430km / London 358km / Berlin 932km / Stockholm 811km
--         合計 2,531km ≒ 上の EMEA の route_km 2,530km（丸めの分だけ差が出る）
SELECT
    region,
    city,
    event_date,
    ROUND(ST_DISTANCE(
        ST_MAKEPOINT(
            LAG(ST_X(geo_point)) OVER (PARTITION BY region ORDER BY event_date),
            LAG(ST_Y(geo_point)) OVER (PARTITION BY region ORDER BY event_date)
        ),
        geo_point
    ) / 1000) AS leg_km
FROM event_schedule_geo
WHERE region = 'EMEA'   -- 他リージョンを見るときはここを変える（PARTITION BY があるので外しても計算は正しい）
ORDER BY event_date;


-----------------------------------------------------------------------
-- STEP 8: ADVANCED ―― 球面ジオメトリで必ず知っておきたい2つの落とし穴
-----------------------------------------------------------------------

/* 【落とし穴1】球面上のポリゴンは「どちらが内側か」が自明ではない
   平面なら閉じた環の内側は一意に決まりますが、球面では環は球を2つの領域に分けるだけで、
   どちらを内側と見なすかは決まりません。Snowflake はこれを2つの関数で使い分けています。

     ST_MAKEPOLYGON        … 環の向きを無視し、常に「小さいほうの領域」を採用する
     ST_MAKEPOLYGONORIENTED… 環の向きを尊重する（左手側が内側。反時計回りなら内側が小さいほう）

   赤道付近の 10度×10度 の正方形で比べます。
   期待値:
     mp_ccw  1,233,205 km2   mp_cw  1,233,205 km2   ← ST_MAKEPOLYGON は向きを無視するので同じ
     mpo_ccw 1,233,205 km2   mpo_cw 508,832,868 km2 ← 逆回りにすると「地球全体から正方形を抜いた側」
   地球の全表面積は約 510,000,000 km2 なので、mpo_cw は文字通り裏返しの領域です。
   意図せず時計回りの環を ST_MAKEPOLYGONORIENTED に渡すと、こうして地球規模の誤りになります。 */
SELECT
    ROUND(ST_AREA(ST_MAKEPOLYGON(        TO_GEOGRAPHY('LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)'))) / 1e6) AS mp_ccw,
    ROUND(ST_AREA(ST_MAKEPOLYGON(        TO_GEOGRAPHY('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)'))) / 1e6) AS mp_cw,
    ROUND(ST_AREA(ST_MAKEPOLYGONORIENTED(TO_GEOGRAPHY('LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)'))) / 1e6) AS mpo_ccw,
    ROUND(ST_AREA(ST_MAKEPOLYGONORIENTED(TO_GEOGRAPHY('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)'))) / 1e6) AS mpo_cw;

/* 【落とし穴2】GEOGRAPHY と GEOMETRY は「単位」が違う
   GEOGRAPHY … 経緯度（SRID 4326）を球面として扱う。距離はメートル、面積は平方メートル。
   GEOMETRY  … 単なる平面座標。同じ経緯度を入れても、距離は「度」の平面ユークリッド距離になり、
               地理的な距離としては意味を持ちません。
   期待値: geography_km 877 / geometry_planar 11.6441 / srid 4326
   11.6441 は「度」です。うっかり GEOMETRY で距離を出して km だと思い込む事故は非常に多いので、
   ST_SRID や型定義で必ず確認します。 */
/* なお「Paris と Berlin の点を1行に横並びにする」ために
     MAX(IFF(city='Paris', geo_point, NULL))
   と書きたくなりますが、これは通りません（学び⑩と同じ話）:
     002016 (22000): SQL compilation error: Function MAX does not support GEOGRAPHY argument type
   GEOGRAPHY は ARRAY_AGG / ST_COLLECT のような対応済みの集約関数でしか集約できないため、
   ここでは素直に CTE を2つ作って交差結合します。 */
WITH paris AS (
    SELECT geo_point AS pt FROM event_schedule_geo WHERE city = 'Paris'
),
berlin AS (
    SELECT geo_point AS pt FROM event_schedule_geo WHERE city = 'Berlin'
)
SELECT
    ROUND(ST_DISTANCE(p.pt, b.pt) / 1000)                          AS geography_km,
    ROUND(ST_DISTANCE(TO_GEOMETRY(p.pt), TO_GEOMETRY(b.pt)), 4)    AS geometry_planar,
    ST_SRID(TO_GEOMETRY(p.pt))                                     AS srid
FROM paris p, berlin b;


-----------------------------------------------------------------------
-- STEP 9: 最終成果物
-----------------------------------------------------------------------

-- リージョンごとに「ポリゴン（角度ソート）」と「開催日順の巡回ルート」を1行にまとめる
SELECT
    a.region,
    e.cities,
    e.first_event,
    e.last_event,
    ROUND(ST_AREA(a.region_polygon) / 1e6) AS area_km2,
    ROUND(ST_LENGTH(r.route) / 1000)       AS route_km,
    ST_ASWKT(a.region_polygon)             AS region_polygon_wkt,
    ST_ASWKT(r.route)                      AS tour_route_wkt
FROM region_polygon_angular a
JOIN region_tour_route      r ON a.region = r.region
JOIN (
    SELECT region, COUNT(*) AS cities, MIN(event_date) AS first_event, MAX(event_date) AS last_event
    FROM event_schedule_geo GROUP BY region
) e ON a.region = e.region
ORDER BY a.region;


-----------------------------------------------------------------------
-- STEP 10: 後片付け
-----------------------------------------------------------------------
-- DROP SCHEMA FROSTYFRIDAY.WEEK112;
