import pandas as pd


class Transformer:

    def __init__(self, raw_data):
        self.raw_data = raw_data

    def create_products_table(self):
        """
        Из сырых данных в формате dict создаем Pandas DataFrame products_table
        """

        products_table = {'name': [], 'quantity': []}

        for key in self.raw_data.keys():

            for i in range(len(self.raw_data[key])):
                products_table['name'].append((self.raw_data[key][i]['PRODUCT_NAME']))
                products_table['quantity'].append((self.raw_data[key][i]['QUANTITY']))

        products_table = pd.DataFrame(products_table)
        products_table = products_table.groupby('name', as_index=False)[['quantity']].sum()

        return products_table

    def count_all_deals(self):
        return len(self.raw_data)

    def count_promo(self, products_table):
        """
        Ищем промо-наборы - товары, в названии которых есть слово 'Промо' - все просто
        """

        products_table['promo'] = products_table['name'].str.find('Промо' or 'промо', 0, 400)
        promo = products_table.loc[products_table['promo'] >= 0].copy()
        return promo['quantity'].sum()

    def create_dishes_table(self, products_table):
        """
        Извлекаем номер товары из названия товара
        Номер содержится в скобках - ужин 2(21)
        Создаем таблицу со столбцами : номер товара, продано штук
        """

        products_table['('] = products_table['name'].str.find('(', 0, 400)
        products_table[')'] = products_table['name'].str.find(')', 0, 400)

        products_table = products_table.loc[products_table['('] > 0]
        products_table = products_table.loc[products_table[')'] > 0]

        products_table = products_table.reset_index(drop=True)

        number = []

        for i in range(len(products_table)):

            row = products_table.loc[i]

            start = row['(']
            stop = row[')']
            value = row['name'][start+1:stop]

            try:
                value = int(value)
            except Exception:
                value = -10

            number.append(value)

        products_table['number'] = number
        products_table = products_table.loc[products_table['number'] >= 0]
        products_table = products_table.groupby('number', as_index=False)[['quantity']].sum()

        return products_table

    def run(self):

        products_table = self.create_products_table()
        deals_amount = self.count_all_deals()
        promo_amount = self.count_promo(products_table)
        dishes_table = self.create_dishes_table(products_table)

        return deals_amount, promo_amount, dishes_table