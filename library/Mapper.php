<?php namespace PHPBook\ETL;

abstract class Mapper {

    private $setup;

    public abstract function getValuesHashByExternalEntity($externalEntity);

    public abstract function getNewExternalEntity($localEntity);

    public abstract function getUpdatedExternalEntity($externalEntity, $localEntity);

    public abstract function getValuesHashByLocalEntity($localEntity);

    public abstract function getNewLocalEntity($externalEntity);

    public abstract function getUpdatedLocalEntity($localEntity, $externalEntity);

    public abstract function getBindedLocalEntityWithExternalEntityKeyValue($localEntity, $externalEntityKeyValue);

    public abstract function getBindedLocalEntityWithExternalEntityHashValue($localEntity, $externalEntityHashValue);

    public function __construct($setup) {

        $this->setup = $setup;
    }

    private function getSchema() {

        foreach($this->setup->schemas as $schema) {

            if ($schema->mapper == get_class($this)) {

                return $schema;

            }

        }

    }

    private function setDebug($message) {

        echo ':: ' . (new \DateTime())->format('H:i:s') . ' ETL:' . $message . PHP_EOL;

    }

    private function makeJoin($queryBuilder, $joins) {

        if (count($joins) >= 4) {
            $conditions = $joins[3];
        } else {
            $conditions = null;
        }

        if ((array_key_exists(2, $joins)) and (strtolower($joins[2])) == 'left') {

             $queryBuilder = $queryBuilder
                ->leftJoin($joins[0], $joins[1], $conditions ? \Doctrine\ORM\Query\Expr\Join::WITH : null, $conditions ? $conditions : null)
                ->addSelect($joins[1]);
                

        } else if ((array_key_exists(2, $joins)) and (strtolower($joins[2])) == 'right') {

             $queryBuilder = $queryBuilder
                ->leftJoin($joins[0], $joins[1], $conditions ? \Doctrine\ORM\Query\Expr\Join::WITH : null, $conditions ? $conditions : null)
                ->addSelect($joins[1]);
                

        } else {

            $queryBuilder = $queryBuilder
                ->innerJoin($joins[0], $joins[1], $conditions ? \Doctrine\ORM\Query\Expr\Join::WITH : null, $conditions ? $conditions : null)
                ->addSelect($joins[1]);

        }

        return $queryBuilder;

    } 

    private function localCharge($priority) {

        $schema = $this->getSchema();

        $externalKeysSync = [];

        $offset = 0;

        do {

            $externalEntityManager = \PHPBook\Database\EntityManager::get($this->setup->connections->external->name);

            $localEntityManager = \PHPBook\Database\EntityManager::get($this->setup->connections->local->name);

            $this->setDebug('Loading local entities items with bulk offset ' . $offset . '...');

            $queryLocalEntities = $localEntityManager->createQueryBuilder()
                ->select($schema->local->table)
                ->from($schema->local->entity, $schema->local->table)
                ->orderBy('ABS(' . $schema->local->table . '.' . $schema->local->attributeExternalKey . ')', 'ASC')
                ->addOrderBy('ABS(' . $schema->local->table . '.' . $schema->local->attributeKey . ')', 'ASC')
                ->setMaxResults((int) $schema->bulk)
                ->setFirstResult($offset);

            $offset += $schema->bulk;

            foreach($schema->local->statements->joins as $joins) {

                $queryLocalEntities = $this->makeJoin($queryLocalEntities, $joins);

            };

            foreach($schema->local->statements->conditions as $conditions) {

                $queryLocalEntities = $queryLocalEntities->andWhere($conditions);

            };

            foreach($schema->local->statements->parameters as $parameterName => $parameterValue) {

                list($valueType, $valueData) = explode(':', $parameterValue, 2);

                switch ($valueType) {
                    case 'datetime':
                        $valueParameterParsing = new \DateTime($valueData);
                        break;
                    case 'boolean':
                        $valueParameterParsing = $valueData == 'true' ? true : false;
                        break;
                    case 'integer':
                        $valueParameterParsing = (int) $valueData;
                        break;
                    case 'float':
                        $valueParameterParsing = (float) $valueData;
                        break;
                    default:
                        $valueParameterParsing = $valueData;
                        break;
                };

                $queryLocalEntities = $queryLocalEntities->setParameter($parameterName, $valueParameterParsing);

            };

            $localEntities = new \Doctrine\ORM\Tools\Pagination\Paginator($queryLocalEntities);

            $localEntities = $localEntities->getIterator();

            if (count($localEntities) == 0) {
                $externalStart = 0;
                $externalEnd = 0;
            } else if (count($localEntities) == 1) {
                $externalStart = $localEntities[0];
                $externalEnd = $externalStart;
                $externalStart = $schema->local->methodExternalKey ? (int) $externalStart->{$schema->local->methodExternalKey}() : (int) $externalStart->{$schema->local->attributeExternalKey};
                $externalEnd = $externalStart;
            } else {
                $externalStart = null;
                $externalEnd = null;
                foreach($localEntities as $le) {
                    $valueKey = $schema->local->methodExternalKey ? (int) $le->{$schema->local->methodExternalKey}() : (int) $le->{$schema->local->attributeExternalKey}; 
                    if ($valueKey) {
                        if (($valueKey < $externalStart) or ($externalStart == null)) {
                            $externalStart = $valueKey;
                        }
                        if (($valueKey > $externalEnd) or ($externalEnd == null)) {
                            $externalEnd = $valueKey;
                        }
                    }
                    $valueKey = null;
                };
                if ($externalStart == null) {
                    $externalStart = 0;
                };
                if ($externalEnd == null) {
                    $externalEnd = 0;
                };
            };

            $this->setDebug('Loading external items related in local items with range keys ' . $externalStart . ' - '.$externalEnd . '...');

            $queryExternalEntities = $externalEntityManager->createQueryBuilder()
                ->select($schema->external->table)
                ->from($schema->external->entity, $schema->external->table)
                ->where('ABS(' . $schema->external->table . '.' . $schema->external->attributeKey . ') >= ' . $externalStart)
                ->andWhere('ABS(' . $schema->external->table . '.' . $schema->external->attributeKey . ') <= ' . $externalEnd)
                ->orderBy('ABS(' . $schema->external->table . '.' . $schema->external->attributeKey . ')', 'ASC');

            foreach($schema->external->statements->joins as $joins) {

                $queryExternalEntities = $this->makeJoin($queryExternalEntities, $joins);


            };

            foreach($schema->external->statements->conditions as $conditions) {

                $queryExternalEntities = $queryExternalEntities->andWhere($conditions);

            };

            foreach($schema->external->statements->parameters as $parameterName => $parameterValue) {

                list($valueType, $valueData) = explode(':', $parameterValue, 2);

                switch ($valueType) {
                    case 'datetime':
                        $valueParameterParsing = new \DateTime($valueData);
                        break;
                    case 'boolean':
                        $valueParameterParsing = $valueData == 'true' ? true : false;
                        break;
                    case 'integer':
                        $valueParameterParsing = (int) $valueData;
                        break;
                    case 'float':
                        $valueParameterParsing = (float) $valueData;
                        break;
                    default:
                        $valueParameterParsing = $valueData;
                        break;
                };

                $queryExternalEntities = $queryExternalEntities->setParameter($parameterName, $valueParameterParsing);

            };

            $externalEntities = new \Doctrine\ORM\Tools\Pagination\Paginator($queryExternalEntities);

            $externalEntities = $externalEntities->getIterator();

            $externalEntitiesMap = [];

            foreach($externalEntities as $externalEntity) {

                $getExternalKeyValue = $schema->external->methodKey ? $externalEntity->{$schema->external->methodKey}() : $externalEntity->{$schema->external->attributeKey};

                $externalEntitiesMap[$getExternalKeyValue] = $externalEntity;

            };

            $externalEntities = null;

            $this->setDebug('Merging bulk with local and external items. External:'.count($externalEntitiesMap).'. Local:'.count($localEntities) . '...');

            if (count($localEntities) > 0) {

                foreach($localEntities as $localEntity) {

                    $getLocalKeyValue = $schema->local->methodKey ? $localEntity->{$schema->local->methodKey}() : $localEntity->{$schema->local->attributeKey};
                    $getExternalKeyValue = $schema->local->methodExternalKey ? $localEntity->{$schema->local->methodExternalKey}() : $localEntity->{$schema->local->attributeExternalKey};
                    $getExternalHashValue = $schema->local->methodExternalHash ? $localEntity->{$schema->local->methodExternalHash}() : $localEntity->{$schema->local->attributeExternalHash};

                    if ($getExternalKeyValue) {

                        $externalEntity = array_key_exists($getExternalKeyValue, $externalEntitiesMap) ? $externalEntitiesMap[$getExternalKeyValue] : null;

                        if ($externalEntity) {

                            $externalKeysSync[$getExternalKeyValue] = true;

                            if ($this->getValuesHashByLocalEntity($localEntity) != $this->getValuesHashByExternalEntity($externalEntity)) {

                                if (($this->getValuesHashByExternalEntity($externalEntity) == $getExternalHashValue)
                                    and
                                    ($this->getValuesHashByLocalEntity($localEntity) != $getExternalHashValue)) {

                                    if (!in_array('dispatch-update', $schema->local->operations->ignore)) {

                                        $externalEntity = $this->getUpdatedExternalEntity($externalEntity, $localEntity);

                                        $externalEntityManager->merge($externalEntity);

                                        $externalEntityManager->flush();

                                        $externalEntityManager->clear();

                                    }

                                } elseif (($this->getValuesHashByExternalEntity($externalEntity) != $getExternalHashValue)
                                    and
                                    ($this->getValuesHashByLocalEntity($localEntity) == $getExternalHashValue)) {

                                    $localEntity = $this->getUpdatedLocalEntity($localEntity, $externalEntity);

                                } else {

                                    if ($priority == \PHPBook\ETL\Routine::$PRIORITY_LOCAL) {

                                        if (!in_array('dispatch-update', $schema->local->operations->ignore)) {

                                            $externalEntity = $this->getUpdatedExternalEntity($externalEntity, $localEntity);

                                            $externalEntityManager->merge($externalEntity);

                                            $externalEntityManager->flush();

                                            $externalEntityManager->clear();

                                        }

                                    } else {

                                        $localEntity = $this->getUpdatedLocalEntity($localEntity, $externalEntity);

                                    }

                                }    

                                $localEntity = $this->getBindedLocalEntityWithExternalEntityHashValue($localEntity, $this->getValuesHashByExternalEntity($externalEntity));

                                $localEntityManager->merge($localEntity);

                                $localEntityManager->flush();  

                                $externalEntityManager->detach($externalEntity);

                                $localEntityManager->detach($localEntity);

                            } else {

                                $localEntityManager->detach($localEntity);

                                $externalEntityManager->detach($externalEntity);

                            }

                        } else {

                            $localEntityManager->remove($localEntity);

                            $localEntityManager->flush();

                        }

                    } else {

                        if (!in_array('dispatch-insert', $schema->local->operations->ignore)) {

                            $externalEntity = $this->getNewExternalEntity($localEntity);

                            $externalEntityManager->persist($externalEntity);

                            $externalEntityManager->flush();

                            $externalEntityManager->clear();

                            $getExternalKeyValue = $schema->external->methodKey ? $externalEntity->{$schema->external->methodKey}() : $externalEntity->{$schema->external->attributeKey};

                            $localEntity = $this->getBindedLocalEntityWithExternalEntityKeyValue($localEntity, $getExternalKeyValue);

                            $localEntity = $this->getBindedLocalEntityWithExternalEntityHashValue($localEntity, $this->getValuesHashByExternalEntity($externalEntity));

                            $localEntityManager->merge($localEntity);

                            $localEntityManager->flush();

                            $externalEntityManager->detach($externalEntity);

                            $localEntityManager->detach($localEntity);

                            $externalKeysSync[$getExternalKeyValue] = true;

                        } else {

                            $localEntityManager->detach($localEntity);

                        }

                    }

                }
            };

            $externalEntitiesMap = null;

        } while(count($localEntities) > 0);

        $this->setDebug('Deleting external rows already deleted in local database...');

        if (!in_array('dispatch-delete', $schema->local->operations->ignore)) {

            $this->localChargeDeleteExternal($externalKeysSync);
            
        }

        return $externalKeysSync;

    }

    private function localChargeDeleteExternal($externalKeysSync) {

        $schema = $this->getSchema();

        $location = ($this->setup->storage->subdirectory ? $this->setup->storage->subdirectory . '/' : '') . 'entities/changes/';

        $filename = $location . '/' . $schema->name . '.json';

        $externalEntityManager = \PHPBook\Database\EntityManager::get($this->setup->connections->external->name);

        $contents =  (new \PHPBook\Storage\Storage)
            ->setConnectionCode($this->setup->storage->name)
            ->setFile($filename)
            ->get();

        $keys = explode(',', $contents);

        if ($keys) {

            foreach($keys as $key) {

                if (strlen($key) > 0) {

                    if (!array_key_exists($key, $externalKeysSync)) {

                        $queryExternalEntities = $externalEntityManager->createQueryBuilder()
                            ->select($schema->external->table)
                            ->from($schema->external->entity, $schema->external->table)
                            ->where($schema->external->table . '.' . $schema->external->attributeKey . ' = :key')
                            ->setParameter(':key', $key)
                            ->orderBy($schema->external->table . '.' . $schema->external->attributeKey, 'ASC');

                        foreach($schema->external->statements->joins as $joins) {

                            $queryExternalEntities = $this->makeJoin($queryExternalEntities, $joins);

                        };

                        foreach($schema->external->statements->conditions as $conditions) {

                            $queryExternalEntities = $queryExternalEntities->andWhere($conditions);

                        };

                        foreach($schema->external->statements->parameters as $parameterName => $parameterValue) {

                            list($valueType, $valueData) = explode(':', $parameterValue, 2);

                            switch ($valueType) {
                                case 'datetime':
                                    $valueParameterParsing = new \DateTime($valueData);
                                    break;
                                case 'boolean':
                                    $valueParameterParsing = $valueData == 'true' ? true : false;
                                    break;
                                case 'integer':
                                    $valueParameterParsing = (int) $valueData;
                                    break;
                                case 'float':
                                    $valueParameterParsing = (float) $valueData;
                                    break;
                                default:
                                    $valueParameterParsing = $valueData;
                                    break;
                            };

                            $queryExternalEntities = $queryExternalEntities->setParameter($parameterName, $valueParameterParsing);

                        };

                        $externalEntity = $queryExternalEntities->getQuery()->getOneOrNullResult();

                        if ($externalEntity) {

                            if (!in_array('dispatch-delete', $schema->local->operations->ignore)) {

                                $externalEntityManager->remove($externalEntity);

                            }

                            $externalEntityManager->flush();

                        }

                    }
                }

            }

        };

    }

    private function externalCharge($externalKeysSync) {

        $schema = $this->getSchema();

        $offsetExternalKey = 0;

        $newsKeysSync = [];

        do {

            $this->setDebug('Loading external items with bulk offset ' . $offsetExternalKey . '...');

            $externalEntityManager = \PHPBook\Database\EntityManager::get($this->setup->connections->external->name);

            $localEntityManager = \PHPBook\Database\EntityManager::get($this->setup->connections->local->name);

            $queryExternalEntities = $externalEntityManager->createQueryBuilder()
                ->select($schema->external->table)
                ->from($schema->external->entity, $schema->external->table)
                ->where('ABS(' . $schema->external->table . '.' . $schema->external->attributeKey . ') > ' . $offsetExternalKey)
                ->orderBy('ABS(' .$schema->external->table . '.' . $schema->external->attributeKey . ')', 'ASC')
                ->setMaxResults((int) $schema->bulk);

            foreach($schema->external->statements->joins as $joins) {

                $queryExternalEntities = $this->makeJoin($queryExternalEntities, $joins);

            };

            foreach($schema->external->statements->conditions as $conditions) {

                $queryExternalEntities = $queryExternalEntities->andWhere($conditions);

            };

            foreach($schema->external->statements->parameters as $parameterName => $parameterValue) {

                list($valueType, $valueData) = explode(':', $parameterValue, 2);

                switch ($valueType) {
                    case 'datetime':
                        $valueParameterParsing = new \DateTime($valueData);
                        break;
                    case 'boolean':
                        $valueParameterParsing = $valueData == 'true' ? true : false;
                        break;
                    case 'integer':
                        $valueParameterParsing = (int) $valueData;
                        break;
                    case 'float':
                        $valueParameterParsing = (float) $valueData;
                        break;
                    default:
                        $valueParameterParsing = $valueData;
                        break;
                };

                $queryExternalEntities = $queryExternalEntities->setParameter($parameterName, $valueParameterParsing);

            };

            $externalEntities = new \Doctrine\ORM\Tools\Pagination\Paginator($queryExternalEntities);

            $externalEntities = $externalEntities->getIterator();

            $this->setDebug('Inserting new external items from bulk in local database...');

            if (count($externalEntities) > 0) {

                $requiresFlush = false;

                $localEntitiesDetach = [];

                foreach($externalEntities as $externalEntity) {

                    $getExternalKeyValue = $schema->external->methodKey ? $externalEntity->{$schema->external->methodKey}() : $externalEntity->{$schema->external->attributeKey};

                    if (!array_key_exists($getExternalKeyValue, $externalKeysSync)) {

                        $requiresFlush = true;

                        $localEntity = $this->getNewLocalEntity($externalEntity);

                        $localEntity = $this->getBindedLocalEntityWithExternalEntityKeyValue($localEntity, $getExternalKeyValue);

                        $localEntity = $this->getBindedLocalEntityWithExternalEntityHashValue($localEntity, $this->getValuesHashByExternalEntity($externalEntity));
                        
                        $localEntityManager->persist($localEntity);

                        $localEntitiesDetach[] = $localEntity;

                        $newsKeysSync[$getExternalKeyValue] = true; 

                    }

                    $externalEntityManager->detach($externalEntity);

                    if ((int) $getExternalKeyValue > (int) $offsetExternalKey) {

                        $offsetExternalKey = (int) $getExternalKeyValue;
                        
                    }

                }

                if ($requiresFlush) {

                    $localEntityManager->flush();

                    foreach($localEntitiesDetach as $localEntityDetach) {

                        $localEntityManager->detach($localEntityDetach);

                    }

                    $localEntityManager->clear();

                    $externalEntityManager->clear();

                }

                $localEntitiesDetach = [];

                $externalEntities = [];


            } else {

                $offsetExternalKey = '-1';

            }

        } while($offsetExternalKey !== '-1');

        $this->setDebug('Generating keys hash files...');

        $location = ($this->setup->storage->subdirectory ? $this->setup->storage->subdirectory . '/' : '') . 'entities/changes/';

        $filename = $location . '/' . $schema->name . '.json';

        (new \PHPBook\Storage\Storage)
            ->setConnectionCode($this->setup->storage->name)
            ->setFile($filename)
            ->write(implode(',',array_keys($externalKeysSync)).','.implode(',',array_keys($newsKeysSync)));

    }

    public function run($priority) {

        $this->setDebug('---------------------- Started First Step: UPDATE/INSERT/DELETE IN LOCAL DB AND EXTERNAL DB BASED ON LOCAL DB');
        $externalKeysSync = $this->localCharge($priority);
        $this->setDebug('---------------------- First Step Done');

        $this->setDebug('---------------------- Started Final Step: INSERT IN LOCAL DB BASED ON EXTERNAL DB IGNORING ITEMS ALREADY INSERTED/UPDATED IN LOCAL DB');
        $this->externalCharge($externalKeysSync);
        $this->setDebug('---------------------- Final Step Done');

    }


}