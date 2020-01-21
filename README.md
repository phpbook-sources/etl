    
+ [About ETL](#about-etl)
+ [Composer Install](#composer-install)
+ [ETL Mapper Example](#etl-mapper-example)
+ [ETL Schema Example](#etl-schema-example)
+ [ETL Loader Example](#etl-loader-example)
+ [ETL Run Example](#etl-run-example)

### About ETL

- A lightweight ETL PHP library.
- The local/external connections and entities are in the PHPBOOK/DATABASE. Require PHPBOOK/DATABASE.
- The storage is in the PHPBOOK/STORAGE to store temporary informations. Require PHPBOOK/STORAGE.
- Provide a routine to exchange data between local and external databases with inserts, updates and deletes.

### Composer Install

	composer require phpbook/etl

### ETL Mapper Example

```php

    class Customer extends \PHPBook\ETL\Mapper {

        public function getValuesHashByExternalEntity($externalEntity) {

            //return a hash of data values based on the external entity. 
            //equals local and external entity values must return same hash
            //do not use id in this hash
            return md5($externalEntity->name . $externalEntity->description);
        }

        public function getNewExternalEntity($localEntity) {

            //new external entity. Do not need pass the external value to the external entity
            $externalEntity = new \Customer\ETL\ERP\Entity\Customer();
            $externalEntity->name = $localEntity->name;
            $externalEntity->description = $localEntity->description;

            return $externalEntity;

        }

        public function getUpdatedExternalEntity($externalEntity, $localEntity) {

            //edit external entity. Do not need pass the external value to the external entity
            $externalEntity->name = $localEntity->name;
            $externalEntity->description = $localEntity->description;

            return $externalEntity;
        }

        public function getValuesHashByLocalEntity($localEntity) {
            
            //return a hash of data values based on the local entity. 
            //equals local and external entity values must return same hash
            //do not use id, integration id/hash in this hash
            return md5($localEntity->name . $localEntity->description);

        }

        public function getNewLocalEntity($externalEntity) {

            //new local entity. Do not need pass the external value key/hash in this time because the bind method will be called
            $localEntity = new \Customer\Entity\Customer();
            $localEntity->name = $localEntity->name;
            $localEntity->description = $localEntity->description;

            return $localEntity;

        }

        public function getUpdatedLocalEntity($localEntity, $externalEntity) {

            //edit local entity. Do not need pass the external value key/hash in this time because the bind method will be called
            $localEntity->name = $externalEntity->name;
            $localEntity->description = $externalEntity->description;

            return $localEntity;
        }

        public function getBindedLocalEntityWithExternalEntityKeyValue($localEntity, $externalEntityKeyValue) {

            //$externalEntityKeyValue string or integer value

            //set the external key value in the local entity row
            $localEntity->external_key = $externalEntityKeyValue; 

            return $localEntity;
        }

        public function getBindedLocalEntityWithExternalEntityHashValue($localEntity, $externalEntityHashValue) {

            //$externalEntityHashValue string value
            
            //set the external hash value in the local entity row
            $localEntity->external_hash = $externalEntityHashValue;

            return $localEntity;

        }

    }


```

### ETL Schema Example

```json
        
    {
        "name": "myETL",
        "storage": {
            "name": "myETL"
        },
        "connections": {
            "local": {
                "name": "default"
            },
            "external": {
                "name": "etl"
            }
        },
        "schemas": [
            {
                "name": "Customer",
                "description": "Customer",
                "mapper": "Customer\\ETL\\ERP\\Mapper\\Customer",
                "bulk": "5000",
                "local": {
                    "entity": "Customer\\Entity\\Customer",
                    "table": "customer",
                    "attributeKey": "id",
                    "attributeExternalKey": "external_key",
                    "attributeExternalHash": "external_hash",
                    "methodKey": "getId",
                    "methodExternalKey": "getExternalKey",
                    "methodExternalHash": "getExternalHash",
                    "statements": {
                        "joins": [["customer.type", "typeAlias"], ["customer.address", "addressAlias"]],
                        "parameters": {"name": "string:name", "birthday": "datetime:-1 year", "cost": "float:500.50", "age": "integer:10", "active": "boolean:true"},
                        "conditions": ["customer.age >= :age", "customer.active = :active", "addressAlias.street like '%street%'"]
                    },
                    "operations": {
                      "ignore": ["dispatch-delete", "dispatch-insert", "dispatch-update"]
                    }
                },
                "external": {
                    "entity": "Customer\\ETL\\ERP\\Entity\\Customer",
                    "table": "customer",
                    "attributeKey": "id",
                    "methodKey": false,
                    "statements": {
                        "joins": [["customer.type", "typeAlias", "left", "typeAlias.group = 10"], ["customer.address", "addressAlias"]],
                        "parameters": {"name": "string:name", "birthday": "datetime:-1 year", "cost": "float:500.50", "age": "integer:10", "active": "boolean:true"},
                        "conditions": ["customer.age >= :age", "customer.active = :active", "addressAlias.street like '%street%'"]
                    }
                }
            }
        ]
    }

        
```

### ETL Loader Example

```php

\PHPBook\ETL\Configuration\Setup::setSetup((new \PHPBook\ETL\Setup())
    ->setFiles(['schema json file path 1', 'schema json file path 2'])
    ->setExceptionCatcher(function(String $message) {
        //the PHPBook ETL does not throw exceptions, but you can take it here
        //you can store $message in database or something else
    }));

?>
```

### ETL Run Example

```php

    $routine = new \PHPBook\ETL\Routine('myETL');

    //PRIORITY_EXTERNAL: when changes are detected in the local database and the external database, use external data
    //PRIORITY_LOCAL: when changes are detected in the local database and the external database, use local data
    $routine->priority(\PHPBook\ETL\Routine::$PRIORITY_EXTERNAL); //default
    $routine->priority(\PHPBook\ETL\Routine::$PRIORITY_LOCAL); 
    
    //run the routine
    $routine->run();


```

You must use primary keys in both databases as a sequencial numbers because the bulk loader get rows sorting by the primary key values and the insert inclusion sort is important in this etl algorithm