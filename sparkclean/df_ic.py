from pyspark.sql.functions import countDistinct, array, lit, col, min, max, UserDefinedFunction
from pyspark.sql.session import SparkSession
import json
from tqdm import tqdm

class ICViolationCorrector:

    def __init__(self, transformer):
        self.spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        self.transformer = transformer
        self.transformer.addPrimaryKey("t_identifier")
        self.df = transformer._df
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
        # Restrict to rules that are defined in only one table
        ic_dict = json.load(open(path))
        # Check input format
        assert(len(ic_dict.keys()) == 1 and list(ic_dict.keys())[0] == 'rules') \
                   ,'Invalid ic input'
        self.number = len(ic_dict['rules'])
        try:
            for i in range(0, self.number):
                type = ic_dict['rules'][i]['type']
                value = ic_dict['rules'][i]['value'][0]
                if type not in self.rules:
                    self.rules[type] = [value]
                else:
                    self.rules[type].append(value)
        except:
            print("Parse error?!")
        for type, valueList in self.rules.items():
            if type == 'fd':
                for value in valueList:
                    LHS, RHS = value.split('|')
                    lhs_attrs = LHS.split(',')
                    for index, attr in enumerate(lhs_attrs):
                        lhs_attrs[index] = attr.strip()
                    rhs_attrs = RHS.split(',')
                    for index, attr in enumerate(rhs_attrs):
                        rhs_attrs[index] = attr.strip()
                    attrs = ['t_identifier'] + lhs_attrs + rhs_attrs
                    if lhs_attrs not in self.lhs_attrs:
                        self.lhs_attrs.append(lhs_attrs)
                    if rhs_attrs not in self.rhs_attrs:
                        self.rhs_attrs.append(rhs_attrs)
                    if attrs not in self.attrs:
                        self.attrs.append(attrs)
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
                else:
                    self.vio_dict[x] = [i]
            if 't_identifier' in self.attrs[i]:
                self.attrs[i].remove('t_identifier')
            self.violation_counts[i] = violation_rows.drop('t_identifier').groupBy(self.attrs[i]).count().orderBy(self.lhs_attrs[i])
        return self

    def display_violation_rows(self):
        num_index = len(self.lhs_attrs)
        for i in range(0, num_index):
            print(self.lhs_attrs[i],' | ', self.rhs_attrs[i])
            self.violation_counts[i].show(self.violation_counts[i].count())

    # def roll_back(self):
    #     self.transformer.replace_sub_df(self.value_max_changes, self.value_min_changes)

    def correct_violations(self, fix_rule):
        if fix_rule == 'single_fd_greedy':
            num_index = len(self.lhs_attrs)
            for index in range(0, num_index):
                keys = self.violation_counts[index].select(self.lhs_attrs[index]).distinct().collect()
                for i in range(0, len(keys)):
                    rhs_values = self.violation_counts[index]
                    for j, c in enumerate(self.lhs_attrs[index]):
                        rhs_values = rhs_values.where(col(c) == keys[i][j])
                    min_changes = rhs_values.select(min('count')).collect()[0][0]
                    max_changes = rhs_values.select(max('count')).collect()[0][0]
                    if min_changes == max_changes:
                        print('This violation cannot be fixed by single_fd_greedy rule')
                        rhs_values.show()
                        continue
                    # Values more likely to be flawed
                    self.value_min_changes = rhs_values.where(col('count') == min_changes). \
                                                        drop(col('count'))
                    # Values less likely to be flawed                                  
                    self.value_max_changes = rhs_values.where(col('count') == max_changes). \
                                                        drop(col('count'))                                                 
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
                            # Find intersected fields of rules 
                            intersection = list(base.intersection(set(self.rhs_attrs[vio_fds[j]])))

                            if len(intersection) > 0:
                                row = self.df.where(col('t_identifier') == id)
                                rhs_values = row.select(intersection).collect()
                                lhs_values = row.select(self.lhs_attrs[vio_fds[i]]).collect()
                                for k in range(0,len(lhs_values)):
                                    for r, c in enumerate(self.lhs_attrs[vio_fds[i]]):
                                        to_select = intersection + ['count']
                                        set_val = self.violation_counts[vio_fds[i]].where(col(c) == lhs_values[0][k]).select(array(to_select)).collect()
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
                                if [rhs_values[0][0]] == to_compare:
                                    continue
                                else:
                                    print(set_val)
                                    print("Field", ','.join(intersection), "with value", ','.join(rhs_values[0]),
                                          "of row {0:d} may be wrong".format(id))
                                    print("Suggested values are:", ','.join(to_compare))
                                    for n,field in enumerate(intersection):
                                        self.transformer.replace_by_id(to_compare[n], field, [id], 't_identifier')
                                fields = fields + intersection
                                
            if len(fields) == 0:
                print('holistic data repairing might not be a good choice')
        return self








