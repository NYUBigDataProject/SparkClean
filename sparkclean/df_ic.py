from pyspark.sql.functions import countDistinct, array, lit, col, min, max, UserDefinedFunction
from pyspark.sql.session import SparkSession
import json
from tqdm import tqdm

class ICViolationCorrector:

    def __init__(self, transformer):
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.transformer = transformer
        self.transformer.addPrimaryKey("t_identifier")
        # self.transformer.addCount()
        self.df = transformer._df
        # Necessary or not
        self.rules = {}
        self.violation = {}
        self.violation_counts = {}
        self.lhs_attrs = list()
        self.rhs_attrs = list()
        self.attrs = list()
        self.vio_dict = {}
        self.value_min_changes = None
        self.value_max_changes = None

    def parse_ic(self, path):
        # Restrict to rules that involve in only one table
        ic_dict = json.load(open(path))
        assert(len(ic_dict.keys()) == 1 and list(ic_dict.keys())[0] == 'rules') \
                   ,'Invalid ic input'
        self.number = len(ic_dict['rules'])
        # print(self.number)
        try:
            for i in range(0, self.number):
                # print(i)
                type = ic_dict['rules'][i]['type']
                value = ic_dict['rules'][i]['value'][0]
                if type not in self.rules:
                    self.rules[type] = [value]
                else:
                    self.rules[type].append(value)
            # print(self.rules)
        except:
            print("Parse error?!")

        for type, valueList in self.rules.items():
            if type == 'fd':
                for value in valueList:
                    LHS, RHS = value.split('|')
                    lhs_attrs = LHS.split(',')
                    for index, attr in enumerate(lhs_attrs):
                        lhs_attrs[index] = attr.strip()
                    # print(lhs_attrs)
                    rhs_attrs = RHS.split(',')
                    for index, attr in enumerate(rhs_attrs):
                        rhs_attrs[index] = attr.strip()
                    attrs = ['t_identifier'] + lhs_attrs + rhs_attrs
                    # Need to convert lhs_attrs to array?
                    # print(self.lhs_attrs)
                    if lhs_attrs not in self.lhs_attrs:
                        self.lhs_attrs.append(lhs_attrs)
                    if rhs_attrs not in self.rhs_attrs:
                        self.rhs_attrs.append(rhs_attrs)
                    if attrs not in self.attrs:
                        self.attrs.append(attrs)
            # if type == 'cfd':


        # print(self.lhs_attrs)
        # print(self.rhs_attrs)
        # print(self.attrs)

        return self

    def check_violations(self):
        # refresh df, in case it has been changed
        self.df = self.transformer._df
        num_index = len(self.lhs_attrs)
        self.vio_dict = {}
        for i in range(0, num_index):
            if 't_identifier' not in self.attrs[i]:
                self.attrs[i].append('t_identifier')
            groupedData = self.df.select(self.attrs[i]).groupBy(self.lhs_attrs[i])
            tmp = groupedData.agg(countDistinct(array(self.rhs_attrs[i])).alias('num'))
            self.violation[i] = tmp.where(tmp.num > 1)
            violation_rows = self.df.join(self.violation[i], self.lhs_attrs[i],
                                          'inner').select(self.attrs[i]).withColumn('redundant', lit(0))

            identifier_collection = violation_rows.select('t_identifier').collect()
            for k in identifier_collection:
                x = k['t_identifier']
                if x in self.vio_dict:
                    self.vio_dict[x].append(i)
                    # print(vio_dict)
                else:
                    #     print(k)
                    self.vio_dict[x] = [i]


            if 't_identifier' in self.attrs[i]:
                self.attrs[i].remove('t_identifier')

            self.violation_counts[i] = violation_rows.drop('t_identifier').groupBy(self.attrs[i]).count().orderBy(self.lhs_attrs[i])
        # for id in self.vio_dict:
        #     print(id)
        return self

    def display_violation_rows(self):
        # for id in self.vio_dict:
        #     print(id)
        num_index = len(self.lhs_attrs)
        for i in range(0, num_index):
            print(self.lhs_attrs[i],' | ', self.rhs_attrs[i])
            self.violation_counts[i].show(self.violation_counts[i].count())
            # print(self.violation_counts[i].collect())

    # def delete_violation(self): which pair of violation? intersect tuples of multiple
    # violations, interaction?

    # def roll_back(self):
    #     self.transformer.replace_sub_df(self.value_max_changes, self.value_min_changes)

    def correct_violations(self, fix_rule):
        # print(self.vio_dict)
        # print('aaaaaaaaaaaaaaaaa')
        if fix_rule == 'single_fd_greedy':
            num_index = len(self.lhs_attrs)
            for index in range(0, num_index):
                keys = self.violation_counts[index].select(self.lhs_attrs[index]).distinct().collect()
                # print(keys)

                for i in range(0, len(keys)):
                    rhs_values = self.violation_counts[index]
                    # rhs_values.show()
                    for j, c in enumerate(self.lhs_attrs[index]):
                        # print(j,c)
                        # print(keys[i][j])
                        rhs_values = rhs_values.where(col(c) == keys[i][j])
                        # print(j,c,keys[i][j])
                    # rhs_values.show()
                    min_changes = rhs_values.select(min('count')).collect()[0][0]
                    max_changes = rhs_values.select(max('count')).collect()[0][0]
                    if min_changes == max_changes:
                        print('This violation cannot be fixed by single_fd_greedy rule')
                        rhs_values.show()
                        continue
                    self.value_min_changes = rhs_values.where(col('count') == min_changes). \
                                                        drop(col('count'))
                                                        # drop(col('count')).collect()
                    self.value_max_changes = rhs_values.where(col('count') == max_changes). \
                                                        drop(col('count'))
                                                      # drop(col('count')).collect()
                    print("Modify:")
                    self.value_min_changes.show(self.value_min_changes.count())
                    print("To:")
                    self.value_max_changes.show(self.value_max_changes.count())
                    self.transformer.replace_sub_df(self.value_min_changes,self.value_max_changes, self.lhs_attrs[index])
        if fix_rule == 'holistic':
            fields = []
            # print(self.vio_dict)
            for id in tqdm(self.vio_dict):
                # print(id)
                vio_fds = self.vio_dict[id]
                if len(vio_fds) > 1:
                    len_vio_fds = len(vio_fds)
                    set_val = []
                    for i in range(0, len_vio_fds-1):
                        for j in range(i+1, len_vio_fds):
                            base = set(self.rhs_attrs[vio_fds[i]])
                            intersection = list(base.intersection(set(self.rhs_attrs[vio_fds[j]])))

                            if len(intersection) > 0:
                                # print(intersection)
                                row = self.df.where(col('t_identifier') == id)
                                rhs_values = row.select(intersection).collect()
                                lhs_values = row.select(self.lhs_attrs[vio_fds[i]]).collect()
                                for k in range(0,len(lhs_values)):
                                    for r, c in enumerate(self.lhs_attrs[vio_fds[i]]):
                                        # print(r,c)
                                        # print(lhs_values[0][k])
                                        # print('a')
                                        to_select = intersection + ['count']
                                        # print(to_select)
                                        # self.violation_counts[vio_fds[i]].show()
                                        set_val = self.violation_counts[vio_fds[i]].where(col(c) == lhs_values[0][k]).select(array(to_select)).collect()
                                # set_val = self.violation_counts[vio_fds[i]].where(self.lhs_attrs[vio_fds[i]]==lhs_values)select(intersection).collect()
                                # val_to_correct = set_val.remove(rhs_values)
                                count = 0
                                less_likely = None
                                item_count = 0
                                for k,item in enumerate(set_val):
                                    item_count = int(item[0][len(intersection)])
                                    # print(item_count)
                                    if item_count > count:
                                        count = item_count
                                        less_likely = k
                                if less_likely == None:
                                    continue
                                to_compare = set_val[less_likely][0][:-1]
                                # print(to_compare)
                                # print(rhs_values[0][0])
                                if [rhs_values[0][0]] == to_compare:
                                    continue
                                else:
                                    # print(rhs_values[0][0])
                                    print(set_val)
                                    print("Field", ','.join(intersection), "with value", ','.join(rhs_values[0]),
                                          "of row {0:d} may be wrong".format(id))
                                    print("Suggested values are:", ','.join(to_compare))
                                    for n,field in enumerate(intersection):
                                        self.transformer.replace_by_id(to_compare[n], field, [id], 't_identifier')

                                # print("intersection:",intersection)
                                fields = fields + intersection
                                # print("fields:",fields)
            if len(fields) == 0:
                print('holistic data repairing might not be a good choice')
        return self



                    # print(value_max_changes[0])
                    # names = value_max_changes.schema.names
                    # value_min_changes = value_min_changes.collect()
                    # value_max_changes = value_max_changes.collect()
                    # print(value_min_changes)
                    # print(value_max_changes)
                    # for k,column in enumerate(names):
                    #     print(value_max_changes[0][k])
                    #     self.transformer.lookup(column, value_max_changes[0][k], value_min_changes[0][k])

                    # for k in range(0, len(value_max_changes[0])):
                    #     udf = UserDefinedFunction(lambda x: value_max_changes[0][k] if x == value_min_changes[0][k] else x)
                    #     col_replace = value_max_changes[0].__fields__[k]
                    #     new_df = new_df.select(*[udf(column).alias(col_replace) if column == col_replace \
                    #                                      else column for column in new_df.columns])
            # self.df = new_df
            #self.df.show()






