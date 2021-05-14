DROP TABLE if exists "DIM_DATE";

CREATE TABLE "DIM_DATE" (
	id INTEGER PRIMARY KEY,
	"Date" DATE,
	"Day" TEXT,
	"Week" INTEGER,
	"Quarter" INTEGER,
	"Year" INTEGER,
	"Year_half" INTEGER,
	"Integration_Timestamp" DATETIME
);


DROP TABLE if exists "DIM_CITY";

CREATE TABLE "DIM_CITY" (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	"City_Siren" VARCHAR(9),
	"City_Insee_Code" VARCHAR(5),
	"City_Name" TEXT,
	"City_Population" INTEGER,
	"City_EPCI_Siren" VARCHAR(9),
	"City_EPCI_Name" TEXT,
	"Integration_Timestamp" DATETIME
);

CREATE UNIQUE INDEX idx_siren_code_dim_city
ON DIM_CITY (City_Siren);

DROP TABLE if exists "FACT_BUDGET_BALANCE_ACCOUNT";

CREATE TABLE "FACT_BUDGET_BALANCE_ACCOUNT" (
	"City_Id" INTEGER NOT NULL,
	"Date_Id" INTEGER NOT NULL,
	"Budget_Type" VARCHAR(30),
	"Budget_Name" TEXT,
	"Operating_Expense_Amount" DECIMAL(12,2),
	"Operating_Income_Amount" DECIMAL(12,2),
	"Invest_Expense_Amount" DECIMAL(12,2),
	"Invest_Income_Amount" DECIMAL(12,2),
	"Gross_Saving" DECIMAL(12,2),
	"Integration_Timestamp" DATETIME
);

