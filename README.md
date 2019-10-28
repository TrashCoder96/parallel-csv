# parallel-csv
Processing csv files collecting results to map with linked lists

Признаю, что 
1) По части обработки ошибок открытия файла и парсинга значений я допустил многие вольности. 
1) Я не сделал лимитер на количество одновременных открытий файлов в
операционной системе (потому что её ресурсы ограничены).
2) Функция createResultCsv() содержит много дублирующего кода, который хотелось бы сжать.
3) Связный список для синхронизирующей структуры мне показалась не очень хорошей идеей, но зато самой быстро реализуемой, 
хотел воспользоваться чем-то вроде b-tree-plus, но времени не хватило.
4) Я не написал тесты и бенчмарки для различных кейсов. Особенно было бы важно показать производительность с помощью бенчарка.
5) Нет пока уверенности, что расход памяти не увеливается с ростом количества и объемов файлов.

Всё это не получилось по причине нехватки времени, но я еще допиливаю.
