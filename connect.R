library(tidyverse)
library(RJDBC)
library(keyring)

options(java.parameters = "-Xmx8048m")
# memory.limit(size=10000000000024)

jdbcDriver <- JDBC(driverClass="com.sap.db.jdbc.Driver",
                   classPath="C:/Program Files/sap/hdbclient/ngdbc.jar")

jdbcConnection <- dbConnect(jdbcDriver,
                            "jdbc:sap://vlpbid001.cokeonena.com:30015/_SYS_BIC",
                            "fl014036",
                            key_get("hana.pw"))

# Fetch all results

sql <- "SELECT \"Customer\",
\"LEOFixLoadDuration\",
\"LEOFixUnloadTime\",
\"Merchandisinglevel\",
\"Name1\",
\"SalesOfficeDesc\"
        FROM \"cona-mdm::Q_CA_R_MDM_Customer_LEOData\" WHERE (1 <> 0)"

leodata <- dbGetQuery(jdbcConnection, sql)

sql <- "SELECT \"CustomerNumber\",
\"Factor1\",
\"Factor2\"
        FROM \"ccf-edw.self-service.delivery-routing::Q_CA_B_TRP_EXPORT\" WHERE (1 <> 0)"

trp <- dbGetQuery(jdbcConnection, sql)

trp.data <- leodata %>%
  unique() %>%
  inner_join(trp %>% unique(), by = c("Customer" = "CustomerNumber"))

sql <- "SELECT
\"ShipTo\",
\"ShipToText\",
\"LEOPlan\",
\"VehicleName\",
\"DeliveryDatePlanned\",
\"CasesPlanned\",
\"ServiceTimePlanned\" FROM
\"ccf-edw.self-service.DeliveryExecution::Q_CA_R_LEOPLAN\" WHERE
\"DeliveryDatePlanned\" >= ? AND
\"DeliveryDatePlanned\" <= ?"

param1 = "2021-01-01"
param2 = "2021-02-28"

leoplan <- dbGetQuery(jdbcConnection, sql, param1, param2)

leoplan.trp <- leoplan %>% left_join(trp.data, by = c("ShipTo" = "Customer"))

leoplan.trp <- leoplan.trp %>%
  mutate(IOT = ServiceTimePlanned / 60,
         vehicle.type = substr(VehicleName, 1, 3),
         vehicle.type = gsub('[[:digit:]]+', '', vehicle.type),
         factor.ratio = as.numeric(Factor2)/as.numeric(Factor1),
         factor.ratio = ifelse(factor.ratio == "Inf", 1.2, factor.ratio)) %>%
  replace_na(list(factor.ratio = 1.2)) %>%
  filter(vehicle.type == "RPD" | vehicle.type == "B",
         Merchandisinglevel != 0,
         factor.ratio >= 1,
         factor.ratio < 3) %>%
  mutate(route.fixed.time = ifelse(vehicle.type == "B", 15, 5))

RPD <- leoplan.trp %>%
  filter(vehicle.type == "RPD") %>%
  mutate(cubes = ceiling(factor.ratio * CasesPlanned),
         trips = ceiling(cubes / 47),
         nonmerch.time = (trips * 4) + 5,
         merch.time = IOT - nonmerch.time)

Bulk <- leoplan.trp %>%
  filter(vehicle.type == "B") %>%
  mutate(cubes = ceiling(factor.ratio * CasesPlanned),
         trips = ceiling(cubes / 100),
         nonmerch.time = (trips * 4) + 15,
         merch.time = IOT - nonmerch.time)

merch.times <- rbind(RPD, Bulk)

sql <- 'SELECT \"CustomerNumber\",
               \"PrimaryGroup\",
               \"PrimaryGroupDesc\",
               \"TradeChannelText\",
               \"ConsSubTradeChannelText\"
         FROM \"cona-mdm::Q_CA_R_MDM_Customer_GeneralSalesArea\"'

mast.cust <- dbGetQuery(jdbcConnection, sql)

merch.times <- merch.times %>%
  left_join(mast.cust, by = c("ShipTo" = "CustomerNumber")) %>%
  rename("Customer" = "ShipTo",
         "Customer Name" = "ShipToText",
         "Delivery Type" = "vehicle.type",
         "Delivery Time" = "nonmerch.time",
         "Merchandising Time" = "merch.time",
         "Key Account" = "PrimaryGroupDesc",
         "Trade Channel" = "TradeChannelText",
         "SubChannel" = "ConsSubTradeChannelText")

write.csv(merch.times, "merchtimes.csv", row.names = FALSE)
