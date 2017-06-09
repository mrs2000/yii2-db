<?php

namespace mrssoft\db;

use Yii;
use yii\base\InvalidParamException;

/**
 * Пакетная вставка / обновление записей
 *
 * @property int $count
 * @property array $data
 * @property array $keys
 */
class DbBatch extends \yii\base\Component
{
    const COMMAND_INSERT = 'insert';
    const COMMAND_REPLACE = 'replace';

    private $data = [];

    public $autoFreeMemory = true;

    /**
     * @var \yii\db\Connection
     */
    public $db;

    /**
     * Максимальное кол-во записей перед вставкой
     * @var int
     */
    public $maxItemsInQuery;

    /**
     * Таблица БД {{%table}}
     * @var string
     */
    public $table;

    /**
     * Комманда INSERT или REPLACE
     * @var string
     */
    public $command = self::COMMAND_INSERT;

    /**
     * Предварительная очистка таблицы
     * @var bool
     */
    public $truncate = false;

    private $isTruncate = false;

    public function init()
    {
        if ($this->db === null) {
            $this->db = self::getDatabase();
        }

        parent::init();
    }

    /**
     * @return \yii\db\Connection
     */
    private static function getDatabase()
    {
        return Yii::$app->db;
    }

    /**
     * Добавить запись
     * @param array $data
     * @param mixed $key
     */
    public function add($data, $key = null)
    {
        if ($key === null) {
            $this->data[] = $data;
        } else {
            $this->data[$key] = $data;
        }

        if ($this->table !== null && $this->maxItemsInQuery !== null && count($this->data) >= $this->maxItemsInQuery) {
            $this->execute();
        }
    }

    /**
     * Удаление записи
     * @param mixed $key
     */
    public function remove($key)
    {
        if (array_key_exists($key, $this->data)) {
            unset($this->data[$key]);
        }
    }

    /**
     * Проверка наличия записи
     * @param $key
     * @return bool
     */
    public function exists($key)
    {
        return array_key_exists($key, $this->data);
    }

    public function getCount()
    {
        return count($this->data);
    }

    /**
     * Плучение значение элемента
     * @param $key
     * @return array|null
     */
    public function get($key)
    {
        if (array_key_exists($key, $this->data)) {
            return $this->data[$key];
        }

        return null;
    }

    public function getData()
    {
        return array_values($this->data);
    }

    /**
     * Массив данных
     * @param array $array
     */
    public function setData(array $array = [])
    {
        $this->data = $array;
    }

    public function getKeys()
    {
        return array_keys($this->data);
    }

    /**
     * Выполнить вставку
     * @return bool
     * @param $table
     * @param bool $truncate
     */
    public function insert($table, $truncate = false)
    {
        if ($truncate) {
            $this->truncate($table);
        }
        return $this->executeCommand('INSERT', $table);
    }

    /**
     * Выполнить замену
     * @return bool
     * @param $table
     * @param bool $truncate
     */
    public function replace($table, $truncate = false)
    {
        if ($truncate) {
            $this->truncate($table);
        }
        return $this->executeCommand('REPLACE', $table);
    }

    /**
     * Выполнить комманду
     * @return bool
     */
    public function execute()
    {
        if ($this->table === null) {
            throw new InvalidParamException('Invalid param: table', 500);
        }

        if (in_array($this->command, [self::COMMAND_INSERT, self::COMMAND_REPLACE], true) === false) {
            throw new InvalidParamException('Invalid param: command', 500);
        }

        $this->{$this->command}($this->table, $this->truncate && !$this->isTruncate);
    }

    /**
     * Обновить запись
     * @param array $data
     * @param mixed $key
     */
    public function update($data, $key)
    {
        if (array_key_exists($key, $this->data)) {
            $this->data[$key] = \yii\helpers\ArrayHelper::merge($this->data[$key], $data);
        }
    }

    /**
     * Вкыл. / выкл. проверку внешних ключей
     * @param bool $value
     * @param \yii\db\Connection $db
     */
    public static function setForeignKey($value, $db = null)
    {
        $val = (bool)$value ? '1' : '0';
        $sql = 'SET FOREIGN_KEY_CHECKS = ' . $val;

        if ($db === null) {
            $db = self::getDatabase();
        }

        $db->createCommand($sql)
           ->execute();
    }

    private function truncate($table)
    {
        $this->db->createCommand()
                 ->truncateTable($table)
                 ->execute();
        $this->isTruncate = true;
    }

    /**
     * Выполнить запрос
     * @param $table
     * @param $command
     * @return bool
     */
    private function executeCommand($command, $table)
    {
        if (empty($this->data)) {
            return false;
        }

        $this->compile($command, $table);
        if ($this->autoFreeMemory) {
            $this->data = [];
            gc_collect_cycles();
        }

        return true;
    }

    /**
     * Сформировать строку запроса
     * @param $command
     * @param $table
     * @return string
     */
    private function compile($command, $table)
    {
        $fields = [];
        foreach (reset($this->data) as $row => $tmp) {
            $fields[] = '`' . $row . '`';
        }

        $n = 0;
        $values = [];
        $pdo = $this->db->getSlavePdo();
        $command = $command . ' INTO ' . $table . ' (' . implode(',', $fields) . ') VALUES ';

        foreach ($this->data as $row) {
            foreach ($row as &$v) {
                $v = $v === null ? 'NULL' : $pdo->quote($v);
            }
            unset($v);
            $values[] = '(' . implode(',', $row) . ')';

            if ($this->maxItemsInQuery !== null && $this->maxItemsInQuery == ++$n) {
                $this->executePartial($command, $values);
                $n = 0;
            }
        }

        if (count($values) > 0) {
            $this->executePartial($command, $values);
        }
    }

    private function executePartial(&$command, &$values)
    {
        $this->db->createCommand($command . implode(',', $values))
                 ->execute();
        $values = [];
        gc_collect_cycles();
    }
}