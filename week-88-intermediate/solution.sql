/************************************************************************************************
 * Frosty Friday Week 88: Snowflake Projection Policies (Intermediate)
 * * 【概要】
 * 特定のカラム（SSN等）に対して「直接の参照（SELECT）は禁止」しつつ、
 * 「データの結合（JOIN）キーとしての利用は許可」するデータガバナンス機能を実装します。
 * * 参考: https://frostyfriday.org/blog/2024/04/05/week-88-intermediate/
 * 公式ドキュメント: https://docs.snowflake.com/en/user-guide/projection-policies
 ************************************************************************************************/

-----------------------------------------------------------------------
-- STEP 1: 環境準備（データベース・テーブル・データの作成）
-----------------------------------------------------------------------
USE SECONDARY ROLES NONE;
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE FROSTYFRIDAY;
CREATE OR REPLACE SCHEMA WEEK88;
USE DATABASE FROSTYFRIDAY;
USE SCHEMA WEEK88;

-- 個人情報管理テーブル
CREATE OR REPLACE TABLE PersonalDetails (
    ID INT PRIMARY KEY,
    FirstName VARCHAR,
    LastName VARCHAR,
    SSN VARCHAR
);

-- 雇用情報管理テーブル（SSNを紐付けキーとして使用）
CREATE OR REPLACE TABLE EmploymentDetails (
    EmploymentID INT PRIMARY KEY,
    SSN VARCHAR,
    CompanyName VARCHAR,
    Position VARCHAR,
    StartDate DATE,
    Salary INT
);

-- データ挿入
INSERT INTO PersonalDetails (ID, FirstName, LastName, SSN) VALUES
    (1, 'John', 'Doe', '123-45-6789'), (2, 'Jane', 'Doe', '987-65-4321'),
    (3, 'Jim', 'Beam', '111-22-3333'), (4, 'Jill', 'Valentine', '444-55-6666'),
    (5, 'Leon', 'Kennedy', '777-88-9999'), (6, 'Claire', 'Redfield', '222-33-4444'),
    (7, 'Chris', 'Redfield', '555-66-7777'), (8, 'Ada', 'Wong', '888-99-0000'),
    (9, 'Albert', 'Wesker', '666-77-8888'), (10, 'Rebecca', 'Chambers', '999-00-1111'),
    (11, 'Barry', 'Burton', '333-44-5555'), (12, 'Carlos', 'Oliveira', '666-55-4444'),
    (13, 'Nikolai', 'Zinoviev', '777-33-2222'), (14, 'Jill', 'Sandwich', '888-44-5555'),
    (15, 'Hunk', 'Unknown', '999-66-7777');

INSERT INTO EmploymentDetails (EmploymentID, SSN, CompanyName, Position, StartDate, Salary) VALUES
    (1, '123-45-6789', 'ACME Corporation', 'Software Engineer', '2018-06-01', 70000),
    (2, '987-65-4321', 'Globex Corporation', 'Project Manager', '2019-08-15', 75000),
    (3, '111-22-3333', 'Soylent Corp', 'Quality Assurance Engineer', '2020-02-01', 68000),
    (4, '444-55-6666', 'Initech', 'IT Support Specialist', '2017-05-23', 62000),
    (5, '777-88-9999', 'Umbrella Corporation', 'Research Scientist', '2021-03-12', 78000),
    (6, '222-33-4444', 'Hooli', 'Data Analyst', '2018-07-01', 69000),
    (7, '555-66-7777', 'Vehement Capital Partners', 'Investment Analyst', '2019-09-09', 71000),
    (8, '888-99-0000', 'Massive Dynamic', 'Executive Assistant', '2020-01-20', 65000),
    (9, '666-77-8888', 'Wayne Enterprises', 'Security Consultant', '2017-04-10', 72000),
    (10, '999-00-1111', 'Stark Industries', 'Mechanical Engineer', '2021-08-05', 83000),
    (11, '333-44-5555', 'Pied Piper', 'Software Developer', '2019-06-01', 85000),
    (12, '666-55-4444', 'Bluth Company', 'Sales Manager', '2018-11-01', 64000),
    (13, '777-33-2222', 'Dunder Mifflin', 'Regional Manager', '2017-12-01', 73000),
    (14, '888-44-5555', 'Los Pollos Hermanos', 'Operations Manager', '2020-07-15', 55000),
    (15, '999-66-7777', 'Cyberdyne Systems', 'Systems Analyst', '2019-04-01', 76000);

-----------------------------------------------------------------------
-- STEP 2: 現状確認（Before）
-----------------------------------------------------------------------
-- 現時点では、誰でもSSNを直接参照できてしまう（ガバナンス上の懸念）
SELECT * FROM PersonalDetails;

-----------------------------------------------------------------------
-- STEP 3: プロジェクション・ポリシーの適用
-----------------------------------------------------------------------

-- 1. カラムの投影（表示）を一律禁止するポリシーを作成
CREATE OR REPLACE PROJECTION POLICY block_ssn_projection
AS () RETURNS PROJECTION_CONSTRAINT
-> PROJECTION_CONSTRAINT(ALLOW => FALSE);

-- 2. SSNカラムにポリシーを適用
ALTER TABLE PersonalDetails 
MODIFY COLUMN SSN SET PROJECTION POLICY block_ssn_projection;

-----------------------------------------------------------------------
-- STEP 4: 動作確認（After）
-----------------------------------------------------------------------

-- 確認A: 直接の参照を試みる
-- 期待値：エラー（Projection policy 'BLOCK_SSN_PROJECTION' prevents...）が発生する
SELECT * FROM PersonalDetails;

-- 確認B: SSNをキーにしてJOINを試みる
-- 期待値：成功。SSN自体をSELECTに含まなければ、内部的な結合処理には使用できる
SELECT 
    p.FirstName, 
    p.LastName, 
    e.CompanyName, 
    e.Position, 
    e.StartDate, 
    e.Salary
FROM PersonalDetails p
JOIN EmploymentDetails e ON p.SSN = e.SSN;

-----------------------------------------------------------------------
-- STEP 5: ADVANCED - ロールベースの動的制御
-- 目的：ACCOUNTADMINのみ参照を許可し、ANALYSTロールには制限をかける
-----------------------------------------------------------------------

-- 1. ANALYSTロールの作成と権限設定
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE ROLE ANALYST;
GRANT USAGE ON DATABASE FROSTYFRIDAY TO ROLE ANALYST;
GRANT USAGE ON SCHEMA WEEK88 TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA WEEK88 TO ROLE ANALYST;

-- 実行中のユーザーにテスト用としてロールを付与
SET MY_USER = CURRENT_USER();
GRANT ROLE ANALYST TO USER IDENTIFIER($MY_USER);

-- 2. 特定のロールのみ許可する条件付きポリシーを作成
-- IS_ROLE_IN_SESSIONを使用し、ACCOUNTADMIN以外は投影をブロックする
CREATE OR REPLACE PROJECTION POLICY role_based_ssn_policy
AS () RETURNS PROJECTION_CONSTRAINT
-> PROJECTION_CONSTRAINT(ALLOW => IS_ROLE_IN_SESSION('ACCOUNTADMIN'));

-- 3. 旧ポリシーを解除し、新しいロールベース・ポリシーを適用
ALTER TABLE PersonalDetails 
MODIFY COLUMN SSN UNSET PROJECTION POLICY;

ALTER TABLE PersonalDetails 
MODIFY COLUMN SSN SET PROJECTION POLICY role_based_ssn_policy;

-----------------------------------------------------------------------
-- STEP 6: ロール別動作検証
-----------------------------------------------------------------------

-- --- 検証1: ANALYSTロールの場合 ---
USE ROLE ANALYST;
USE WAREHOUSE COMPUTE_WH;

-- 確認A: SELECT * はブロックされるか
SELECT * FROM PersonalDetails;

-- 確認B: JOINキーとしては機能するか
SELECT p.FirstName, e.CompanyName
FROM PersonalDetails p
JOIN EmploymentDetails e ON p.SSN = e.SSN;

-- --- 検証2: ACCOUNTADMINロールの場合 ---
USE ROLE ACCOUNTADMIN;

-- 全てのデータ（SSN含む）が参照できるか
SELECT * FROM PersonalDetails;