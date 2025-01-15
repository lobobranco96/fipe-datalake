![data_architecture _png](https://github.com/user-attachments/assets/154b7908-b215-4180-82aa-5d009408e85b)


**SNOWFLAKE SCHEMA**

                     +---------------------+
                     |     brand_dim       |
                     |---------------------|
                     | id_brand            |
                     | brand_name          |
                     +---------------------+
                            |
                            | 1:N
                            |
                     +----------------------+
                     | model_vehicle_dim    |
                     |----------------------|
                     | id_model_vehicle     |
                     | id_brand             |
                     | vehicle_model        |
                     | car_detail           |
                     | cambio               |
                     | cilindrada           |
                     +----------------------+
                            |
                            | 1:N
                            |
                     +---------------------+
                     |      year_dim       |
                     |---------------------|
                     | id_year             |
                     | id_model_vehicle    |
                     | year                |
                     +---------------------+
                            |
                            | 1:N
                            |
                     +---------------------+
                     | fipe_price_fact     |
                     |---------------------|
                     | id_fipe_price       |
                     | id_year             |
                     | id_model_vehicle    |
                     | fipe_price          |
                     +---------------------+

