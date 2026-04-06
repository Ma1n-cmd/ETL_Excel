import pandas as pd
import numpy as np
import os
import asyncio
import random
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

class AsyncDataGenerator:
    def __init__(self, output_dir="test_data"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.products = [f"Товар_{i}" for i in range(1, 1001)]
        self.cities = ["Москва", "СПб", "Казань", "Новосибирск", "Екатеринбург", "Воронеж"]
        self.categories = ["Электроника", "Одежда", "Книги", "Продукты", "Спорт"]

    # Выносим логику генерации в статический метод, чтобы ProcessPool мог его сериализовать
    @staticmethod
    def _generate_single_file(output_dir, products, categories, cities, rows, file_index, date):
        if date is None:
            date = datetime.now() - timedelta(days=random.randint(0, 30))

        data = {
            'id': range(rows * file_index, rows * (file_index + 1)),
            'product': np.random.choice(products, rows),
            'category': np.random.choice(categories, rows),
            'city': np.random.choice(cities, rows),
            'price': np.random.uniform(100, 50000, rows).round(2),
            'quantity': np.random.randint(1, 1000, rows),
            'date_created': [date.strftime('%Y-%m-%d')] * rows,
            'last_updated': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * rows
        }

        df = pd.DataFrame(data)
        filename = f"data_{date.strftime('%Y%m%d')}_{file_index:03d}.xlsx"
        filepath = os.path.join(output_dir, filename)
        
        # Используем engine='xlsxwriter', он обычно быстрее для простых таблиц
        df.to_excel(filepath, index=False, engine='xlsxwriter')
        return filepath

    async def generate_multiple_files_async(self, num_files=100, rows_per_file=10000):
        loop = asyncio.get_running_loop()
        tasks = []

        # Используем ProcessPoolExecutor для параллельных вычислений
        with ProcessPoolExecutor() as pool:
            print(f"Запуск генерации {num_files} файлов в несколько потоков...")
            
            for i in range(num_files):
                date = datetime.now() - timedelta(days=random.randint(0, num_files))
                
                # Планируем выполнение тяжелой задачи в пуле процессов
                task = loop.run_in_executor(
                    pool, 
                    self._generate_single_file, 
                    self.output_dir, self.products, self.categories, self.cities, 
                    rows_per_file, i, date
                )
                tasks.append(task)

            # Ждем завершения всех задач
            results = await asyncio.gather(*tasks)
            print(f"Готово! Создано файлов: {len(results)}")
            return results

if __name__ == "__main__":
    generator = AsyncDataGenerator()
    
    # Запуск асинхронного цикла
    start_time = datetime.now()
    asyncio.run(generator.generate_multiple_files_async(num_files=100, rows_per_file=10000))
    print(f"Затрачено времени: {datetime.now() - start_time}")