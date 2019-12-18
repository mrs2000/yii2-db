yii2-db
=================

Batch insert into database.

![PHP](https://img.shields.io/packagist/php-v/mrssoft/yii2-db.svg)
![Github](https://img.shields.io/github/license/mrs2000/yii2-db.svg)
![Total Downloads](https://img.shields.io/packagist/dt/mrssoft/yii2-db.svg)

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
php composer.phar require --prefer-dist mrssoft/yii2-db "*"
```

or add

```
"mrssoft/yii2-db": "*"
```

to the require section of your `composer.json` file.


Usage
-----

```php
$batch = new DbBatch();

$batch->add([
    'field1' => $value1,
    'field2' => $value2,
]);

$batch->add([
    'field1' => $value1,
    'field2' => $value2,
], $key)

$batch->addUnique([
    'field1' => $value1,
    'field2' => $value2,
]);

$bool = $batch->insert('{{%table}}', true);
$bool = $batch->replace('{{%table}}');

```

```php
$batch = new DbBatch([
    'maxItemsInQuery' => 1000,
    'table' => '{{%table}}',
    'truncate' => true,
    'command' => DbBatch::COMMAND_INSERT
]);

$batch->add([
    'field1' => $value1,
    'field2' => $value2,
], $key);

$batch->update([
    'field1' => $value3,
    'field2' => $value4,
], $key);

$count = $batch->getCount();
$keys = $batch->getKeys();
$element = $batch->get($key);
$data = $batch->getData();
$batch->setData($data);

$bool = $batch->execute();
```