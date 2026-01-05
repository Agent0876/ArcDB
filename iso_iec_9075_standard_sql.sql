-- ============================================================================
-- ISO/IEC 9075:2023 표준 SQL 예제
-- ISO/IEC 9075-1:2023 (SQL/Framework) 문서를 기반으로 작성
-- ============================================================================

-- ============================================================================
-- 1. 스키마 정의 (Schema Definition)
-- ISO/IEC 9075-2 (SQL/Foundation) 기반
-- ============================================================================

-- 스키마 생성
CREATE SCHEMA company_schema
    AUTHORIZATION admin_user;

-- ============================================================================
-- 2. 테이블 정의 (Table Definition)
-- 표준 데이터 타입 사용: CHARACTER, CHARACTER VARYING, INTEGER, DECIMAL, DATE, etc.
-- ============================================================================

-- 부서 테이블
CREATE TABLE company_schema.departments (
    department_id       INTEGER         NOT NULL,
    department_name     CHARACTER VARYING(100) NOT NULL,
    location            CHARACTER VARYING(200),
    budget              DECIMAL(15, 2)  DEFAULT 0.00,
    created_date        DATE            DEFAULT CURRENT_DATE,
    
    -- 기본 키 제약조건
    CONSTRAINT pk_departments PRIMARY KEY (department_id),
    
    -- 유니크 제약조건
    CONSTRAINT uk_department_name UNIQUE (department_name),
    
    -- 체크 제약조건
    CONSTRAINT chk_budget CHECK (budget >= 0)
);

-- 직원 테이블
CREATE TABLE company_schema.employees (
    employee_id         INTEGER         NOT NULL,
    first_name          CHARACTER VARYING(50) NOT NULL,
    last_name           CHARACTER VARYING(50) NOT NULL,
    email               CHARACTER VARYING(100),
    phone_number        CHARACTER(20),
    hire_date           DATE            NOT NULL,
    job_title           CHARACTER VARYING(100),
    salary              DECIMAL(10, 2),
    commission_pct      DECIMAL(5, 2),
    department_id       INTEGER,
    manager_id          INTEGER,
    is_active           BOOLEAN         DEFAULT TRUE,
    
    -- 기본 키 제약조건
    CONSTRAINT pk_employees PRIMARY KEY (employee_id),
    
    -- 외래 키 제약조건 (Referential Integrity)
    CONSTRAINT fk_emp_department 
        FOREIGN KEY (department_id) 
        REFERENCES company_schema.departments(department_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    
    -- 자기 참조 외래 키 (Self-referencing)
    CONSTRAINT fk_emp_manager 
        FOREIGN KEY (manager_id) 
        REFERENCES company_schema.employees(employee_id),
    
    -- 체크 제약조건
    CONSTRAINT chk_salary CHECK (salary > 0),
    CONSTRAINT chk_commission CHECK (commission_pct BETWEEN 0.00 AND 1.00)
);

-- 프로젝트 테이블
CREATE TABLE company_schema.projects (
    project_id          INTEGER         NOT NULL,
    project_name        CHARACTER VARYING(200) NOT NULL,
    description         CHARACTER VARYING(4000),
    start_date          DATE,
    end_date            DATE,
    status              CHARACTER VARYING(20) DEFAULT 'PLANNED',
    department_id       INTEGER,
    
    CONSTRAINT pk_projects PRIMARY KEY (project_id),
    
    CONSTRAINT fk_proj_department 
        FOREIGN KEY (department_id) 
        REFERENCES company_schema.departments(department_id),
    
    -- 날짜 유효성 검사
    CONSTRAINT chk_project_dates CHECK (end_date IS NULL OR end_date >= start_date),
    
    -- 상태 값 제한
    CONSTRAINT chk_project_status CHECK (status IN ('PLANNED', 'ACTIVE', 'COMPLETED', 'CANCELLED'))
);

-- 직원-프로젝트 할당 테이블 (다대다 관계)
CREATE TABLE company_schema.project_assignments (
    assignment_id       INTEGER         NOT NULL,
    employee_id         INTEGER         NOT NULL,
    project_id          INTEGER         NOT NULL,
    role                CHARACTER VARYING(50),
    assigned_date       DATE            DEFAULT CURRENT_DATE,
    hours_allocated     DECIMAL(5, 2)   DEFAULT 0.00,
    
    CONSTRAINT pk_project_assignments PRIMARY KEY (assignment_id),
    
    CONSTRAINT fk_assign_employee 
        FOREIGN KEY (employee_id) 
        REFERENCES company_schema.employees(employee_id)
        ON DELETE CASCADE,
    
    CONSTRAINT fk_assign_project 
        FOREIGN KEY (project_id) 
        REFERENCES company_schema.projects(project_id)
        ON DELETE CASCADE,
    
    -- 복합 유니크 제약조건
    CONSTRAINT uk_employee_project UNIQUE (employee_id, project_id)
);

-- ============================================================================
-- 3. 뷰 정의 (View Definition)
-- ============================================================================

-- 직원 상세 정보 뷰
CREATE VIEW company_schema.employee_details AS
    SELECT 
        e.employee_id,
        e.first_name,
        e.last_name,
        e.first_name || ' ' || e.last_name AS full_name,
        e.email,
        e.hire_date,
        e.job_title,
        e.salary,
        d.department_name,
        m.first_name || ' ' || m.last_name AS manager_name
    FROM 
        company_schema.employees e
    LEFT JOIN 
        company_schema.departments d ON e.department_id = d.department_id
    LEFT JOIN 
        company_schema.employees m ON e.manager_id = m.employee_id
    WHERE 
        e.is_active = TRUE;

-- 부서별 통계 뷰
CREATE VIEW company_schema.department_statistics AS
    SELECT 
        d.department_id,
        d.department_name,
        COUNT(e.employee_id) AS employee_count,
        COALESCE(AVG(e.salary), 0) AS average_salary,
        COALESCE(SUM(e.salary), 0) AS total_salary,
        COALESCE(MIN(e.salary), 0) AS min_salary,
        COALESCE(MAX(e.salary), 0) AS max_salary
    FROM 
        company_schema.departments d
    LEFT JOIN 
        company_schema.employees e ON d.department_id = e.department_id AND e.is_active = TRUE
    GROUP BY 
        d.department_id, d.department_name;

-- ============================================================================
-- 4. 데이터 조작 언어 (DML - Data Manipulation Language)
-- ============================================================================

-- INSERT 문
INSERT INTO company_schema.departments (department_id, department_name, location, budget)
VALUES (1, 'Engineering', 'Building A, Floor 3', 500000.00);

INSERT INTO company_schema.departments (department_id, department_name, location, budget)
VALUES (2, 'Human Resources', 'Building B, Floor 1', 200000.00);

INSERT INTO company_schema.departments (department_id, department_name, location, budget)
VALUES (3, 'Marketing', 'Building A, Floor 2', 350000.00);

-- 다중 행 INSERT
INSERT INTO company_schema.employees (employee_id, first_name, last_name, email, hire_date, job_title, salary, department_id)
VALUES 
    (1, 'John', 'Smith', 'john.smith@company.com', DATE '2020-01-15', 'Senior Engineer', 85000.00, 1),
    (2, 'Jane', 'Doe', 'jane.doe@company.com', DATE '2019-06-20', 'Engineering Manager', 120000.00, 1),
    (3, 'Robert', 'Johnson', 'robert.johnson@company.com', DATE '2021-03-10', 'HR Specialist', 55000.00, 2),
    (4, 'Emily', 'Williams', 'emily.williams@company.com', DATE '2018-09-01', 'Marketing Director', 95000.00, 3);

-- 매니저 관계 설정
UPDATE company_schema.employees
SET manager_id = 2
WHERE employee_id = 1;

-- ============================================================================
-- 5. 쿼리 표현식 (Query Expressions)
-- ============================================================================

-- 기본 SELECT 문
SELECT 
    employee_id,
    first_name,
    last_name,
    salary
FROM 
    company_schema.employees
WHERE 
    salary > 50000.00
ORDER BY 
    salary DESC;

-- JOIN 연산
SELECT 
    e.first_name,
    e.last_name,
    d.department_name,
    p.project_name
FROM 
    company_schema.employees e
INNER JOIN 
    company_schema.departments d ON e.department_id = d.department_id
LEFT OUTER JOIN 
    company_schema.project_assignments pa ON e.employee_id = pa.employee_id
LEFT OUTER JOIN 
    company_schema.projects p ON pa.project_id = p.project_id;

-- 서브쿼리 (Subquery)
SELECT 
    first_name,
    last_name,
    salary
FROM 
    company_schema.employees
WHERE 
    salary > (
        SELECT AVG(salary) 
        FROM company_schema.employees 
        WHERE is_active = TRUE
    );

-- 상관 서브쿼리 (Correlated Subquery)
SELECT 
    d.department_name,
    (
        SELECT COUNT(*) 
        FROM company_schema.employees e 
        WHERE e.department_id = d.department_id
    ) AS employee_count
FROM 
    company_schema.departments d;

-- EXISTS 연산자
SELECT 
    d.department_name
FROM 
    company_schema.departments d
WHERE 
    EXISTS (
        SELECT 1 
        FROM company_schema.employees e 
        WHERE e.department_id = d.department_id 
        AND e.salary > 100000.00
    );

-- CASE 표현식
SELECT 
    first_name,
    last_name,
    salary,
    CASE 
        WHEN salary >= 100000 THEN 'Executive'
        WHEN salary >= 70000 THEN 'Senior'
        WHEN salary >= 50000 THEN 'Mid-Level'
        ELSE 'Junior'
    END AS salary_grade
FROM 
    company_schema.employees;

-- 집합 연산 (UNION, INTERSECT, EXCEPT)
SELECT first_name, last_name FROM company_schema.employees WHERE department_id = 1
UNION
SELECT first_name, last_name FROM company_schema.employees WHERE salary > 90000;

SELECT first_name, last_name FROM company_schema.employees WHERE department_id = 1
INTERSECT
SELECT first_name, last_name FROM company_schema.employees WHERE salary > 80000;

SELECT first_name, last_name FROM company_schema.employees WHERE is_active = TRUE
EXCEPT
SELECT first_name, last_name FROM company_schema.employees WHERE department_id = 2;

-- ============================================================================
-- 6. 집계 함수 (Aggregate Functions)
-- ============================================================================

SELECT 
    d.department_name,
    COUNT(*) AS total_employees,
    COUNT(DISTINCT e.job_title) AS distinct_job_titles,
    AVG(e.salary) AS average_salary,
    SUM(e.salary) AS total_salary,
    MIN(e.salary) AS min_salary,
    MAX(e.salary) AS max_salary
FROM 
    company_schema.employees e
JOIN 
    company_schema.departments d ON e.department_id = d.department_id
GROUP BY 
    d.department_name
HAVING 
    COUNT(*) > 0
ORDER BY 
    total_salary DESC;

-- ============================================================================
-- 7. 윈도우 함수 (Window Functions) - ISO/IEC 9075-2
-- ============================================================================

SELECT 
    first_name,
    last_name,
    department_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dept_rank,
    RANK() OVER (ORDER BY salary DESC) AS company_rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank,
    SUM(salary) OVER (PARTITION BY department_id) AS dept_total_salary,
    AVG(salary) OVER (PARTITION BY department_id) AS dept_avg_salary,
    LEAD(salary, 1) OVER (ORDER BY salary DESC) AS next_lower_salary,
    LAG(salary, 1) OVER (ORDER BY salary DESC) AS next_higher_salary
FROM 
    company_schema.employees;

-- ============================================================================
-- 8. 공통 테이블 표현식 (CTE - Common Table Expressions)
-- ============================================================================

WITH department_summary AS (
    SELECT 
        department_id,
        COUNT(*) AS emp_count,
        AVG(salary) AS avg_salary
    FROM 
        company_schema.employees
    WHERE 
        is_active = TRUE
    GROUP BY 
        department_id
),
high_paying_depts AS (
    SELECT 
        department_id
    FROM 
        department_summary
    WHERE 
        avg_salary > 60000
)
SELECT 
    d.department_name,
    ds.emp_count,
    ds.avg_salary
FROM 
    department_summary ds
JOIN 
    company_schema.departments d ON ds.department_id = d.department_id
JOIN 
    high_paying_depts hpd ON ds.department_id = hpd.department_id;

-- 재귀 CTE (Recursive CTE) - 조직도
WITH RECURSIVE org_hierarchy AS (
    -- 기본 케이스: 최상위 관리자
    SELECT 
        employee_id,
        first_name,
        last_name,
        manager_id,
        1 AS level,
        CAST(first_name || ' ' || last_name AS CHARACTER VARYING(500)) AS hierarchy_path
    FROM 
        company_schema.employees
    WHERE 
        manager_id IS NULL
    
    UNION ALL
    
    -- 재귀 케이스: 부하 직원
    SELECT 
        e.employee_id,
        e.first_name,
        e.last_name,
        e.manager_id,
        oh.level + 1,
        CAST(oh.hierarchy_path || ' > ' || e.first_name || ' ' || e.last_name AS CHARACTER VARYING(500))
    FROM 
        company_schema.employees e
    INNER JOIN 
        org_hierarchy oh ON e.manager_id = oh.employee_id
)
SELECT * FROM org_hierarchy ORDER BY level, last_name;

-- ============================================================================
-- 9. 저장 프로시저 및 함수 (ISO/IEC 9075-4: PSM)
-- ============================================================================

-- 함수 생성
CREATE FUNCTION company_schema.calculate_bonus(
    p_salary DECIMAL(10, 2),
    p_performance_rating INTEGER
)
RETURNS DECIMAL(10, 2)
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
BEGIN
    DECLARE v_bonus_rate DECIMAL(5, 2);
    
    CASE p_performance_rating
        WHEN 5 THEN SET v_bonus_rate = 0.20;
        WHEN 4 THEN SET v_bonus_rate = 0.15;
        WHEN 3 THEN SET v_bonus_rate = 0.10;
        WHEN 2 THEN SET v_bonus_rate = 0.05;
        ELSE SET v_bonus_rate = 0.00;
    END CASE;
    
    RETURN p_salary * v_bonus_rate;
END;

-- 저장 프로시저 생성
CREATE PROCEDURE company_schema.give_raise(
    IN p_employee_id INTEGER,
    IN p_percentage DECIMAL(5, 2),
    OUT p_new_salary DECIMAL(10, 2)
)
LANGUAGE SQL
MODIFIES SQL DATA
BEGIN
    DECLARE v_current_salary DECIMAL(10, 2);
    
    -- 현재 급여 조회
    SELECT salary INTO v_current_salary
    FROM company_schema.employees
    WHERE employee_id = p_employee_id;
    
    -- 새 급여 계산
    SET p_new_salary = v_current_salary * (1 + p_percentage / 100);
    
    -- 급여 업데이트
    UPDATE company_schema.employees
    SET salary = p_new_salary
    WHERE employee_id = p_employee_id;
END;

-- ============================================================================
-- 10. 트리거 (Triggers)
-- ============================================================================

-- 감사 로그 테이블
CREATE TABLE company_schema.audit_log (
    log_id              INTEGER         NOT NULL,
    table_name          CHARACTER VARYING(100),
    operation           CHARACTER VARYING(10),
    old_values          CHARACTER VARYING(4000),
    new_values          CHARACTER VARYING(4000),
    changed_by          CHARACTER VARYING(100),
    changed_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT pk_audit_log PRIMARY KEY (log_id)
);

-- 급여 변경 트리거
CREATE TRIGGER company_schema.trg_salary_audit
    AFTER UPDATE OF salary ON company_schema.employees
    REFERENCING OLD ROW AS old_row NEW ROW AS new_row
    FOR EACH ROW
    WHEN (old_row.salary <> new_row.salary)
BEGIN ATOMIC
    INSERT INTO company_schema.audit_log (
        log_id, table_name, operation, old_values, new_values, changed_by
    )
    VALUES (
        (SELECT COALESCE(MAX(log_id), 0) + 1 FROM company_schema.audit_log),
        'employees',
        'UPDATE',
        'salary: ' || CAST(old_row.salary AS CHARACTER VARYING(20)),
        'salary: ' || CAST(new_row.salary AS CHARACTER VARYING(20)),
        CURRENT_USER
    );
END;

-- ============================================================================
-- 11. 트랜잭션 제어 (Transaction Control)
-- ============================================================================

-- 트랜잭션 시작
START TRANSACTION;

-- SAVEPOINT 설정
SAVEPOINT before_updates;

-- 데이터 수정
UPDATE company_schema.employees
SET salary = salary * 1.05
WHERE department_id = 1;

-- 조건에 따라 롤백 또는 커밋
-- ROLLBACK TO SAVEPOINT before_updates;  -- 롤백할 경우
COMMIT;

-- ============================================================================
-- 12. 인덱스 생성 (Indexes)
-- ============================================================================

CREATE INDEX idx_emp_department 
    ON company_schema.employees (department_id);

CREATE INDEX idx_emp_name 
    ON company_schema.employees (last_name, first_name);

CREATE UNIQUE INDEX idx_emp_email 
    ON company_schema.employees (email);

-- ============================================================================
-- 13. 시퀀스 (Sequences)
-- ============================================================================

CREATE SEQUENCE company_schema.seq_employee_id
    START WITH 1000
    INCREMENT BY 1
    NO MAXVALUE
    NO CYCLE;

-- 시퀀스 사용
INSERT INTO company_schema.employees (
    employee_id, first_name, last_name, email, hire_date, job_title, salary, department_id
)
VALUES (
    NEXT VALUE FOR company_schema.seq_employee_id,
    'New', 'Employee', 'new.employee@company.com', CURRENT_DATE, 'Intern', 35000.00, 1
);

-- ============================================================================
-- 14. 권한 관리 (Authorization)
-- ============================================================================

-- 권한 부여
GRANT SELECT, INSERT, UPDATE ON company_schema.employees TO hr_role;
GRANT SELECT ON company_schema.employee_details TO all_users;
GRANT EXECUTE ON FUNCTION company_schema.calculate_bonus TO hr_role;

-- 권한 취소
REVOKE DELETE ON company_schema.employees FROM hr_role;

-- ============================================================================
-- 15. 임시 테이블 (Temporary Tables)
-- ============================================================================

CREATE LOCAL TEMPORARY TABLE temp_high_earners (
    employee_id     INTEGER,
    full_name       CHARACTER VARYING(100),
    salary          DECIMAL(10, 2)
) ON COMMIT DELETE ROWS;

INSERT INTO temp_high_earners
SELECT 
    employee_id,
    first_name || ' ' || last_name,
    salary
FROM 
    company_schema.employees
WHERE 
    salary > 100000;

-- ============================================================================
-- 16. MERGE 문 (Upsert)
-- ============================================================================

MERGE INTO company_schema.employees AS target
USING (
    SELECT 5 AS employee_id, 'Alice' AS first_name, 'Brown' AS last_name,
           'alice.brown@company.com' AS email, DATE '2023-01-01' AS hire_date,
           'Data Analyst' AS job_title, 65000.00 AS salary, 1 AS department_id
) AS source
ON target.employee_id = source.employee_id
WHEN MATCHED THEN
    UPDATE SET 
        first_name = source.first_name,
        last_name = source.last_name,
        salary = source.salary
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, email, hire_date, job_title, salary, department_id)
    VALUES (source.employee_id, source.first_name, source.last_name, source.email, 
            source.hire_date, source.job_title, source.salary, source.department_id);

-- ============================================================================
-- 17. NULL 처리 함수
-- ============================================================================

SELECT 
    first_name,
    last_name,
    COALESCE(commission_pct, 0.00) AS commission,
    NULLIF(department_id, 0) AS dept_id,
    CASE WHEN salary IS NULL THEN 'N/A' ELSE CAST(salary AS CHARACTER VARYING(20)) END AS salary_str
FROM 
    company_schema.employees;

-- ============================================================================
-- 18. 날짜/시간 함수
-- ============================================================================

SELECT 
    first_name,
    last_name,
    hire_date,
    CURRENT_DATE AS today,
    CURRENT_TIMESTAMP AS now,
    EXTRACT(YEAR FROM hire_date) AS hire_year,
    EXTRACT(MONTH FROM hire_date) AS hire_month,
    hire_date + INTERVAL '1' YEAR AS one_year_anniversary
FROM 
    company_schema.employees;

-- ============================================================================
-- 19. 문자열 함수
-- ============================================================================

SELECT 
    first_name,
    last_name,
    UPPER(last_name) AS upper_name,
    LOWER(first_name) AS lower_name,
    CHAR_LENGTH(email) AS email_length,
    SUBSTRING(email FROM 1 FOR POSITION('@' IN email) - 1) AS email_username,
    TRIM(BOTH ' ' FROM first_name) AS trimmed_name,
    first_name || ' ' || last_name AS full_name
FROM 
    company_schema.employees;

-- ============================================================================
-- 20. 정리 (Cleanup)
-- ============================================================================

-- 삭제 순서: 참조 무결성을 고려하여 역순으로 삭제
DROP VIEW IF EXISTS company_schema.department_statistics;
DROP VIEW IF EXISTS company_schema.employee_details;
DROP TABLE IF EXISTS company_schema.audit_log;
DROP TABLE IF EXISTS company_schema.project_assignments;
DROP TABLE IF EXISTS company_schema.projects;
DROP TABLE IF EXISTS company_schema.employees;
DROP TABLE IF EXISTS company_schema.departments;
DROP SEQUENCE IF EXISTS company_schema.seq_employee_id;
DROP FUNCTION IF EXISTS company_schema.calculate_bonus;
DROP PROCEDURE IF EXISTS company_schema.give_raise;
DROP SCHEMA IF EXISTS company_schema;
