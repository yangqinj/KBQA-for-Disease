"""
@author: Qinjuan Yang
@time: 2022-03-22 00:17
@desc: 
"""
import json
import os
import re

from tqdm import tqdm
import pandas as pd


entity_type_disease = "Disease"
entity_type_symptom = "Symptom"
entity_type_drug = "Drug"
entity_type_check = "Check"
entity_type_department = "Department"
entity_type_food = "Food"
entity_type_recipe = "Recipe"
entity_type_pharm_company = "PharmCompany"
entity_type_treatment = "Treatment"

rels_type_recommend_drug = "RECOMMEND_DRUG"
rels_type_recommend_recipe = "RECOMMEND_RECIPE"
rels_type_avoid_eat = "AVOID_EAT"
rels_type_advise_eat = "ADVISE_EAT"
rels_type_has_symptom = "HAS_SYMPTOM"
rels_type_has_complication = "HAS_COMPLICATION"
rels_type_common_drug = "COMMON_DRUG"
rels_type_need_check = "NEED_CHECK"
rels_type_treat_department = "TREAT_DEPARTMENT"
rels_type_belongs_to = "BELONGS_TO"
rels_type_produce_drug = "PRODUCE_DRUG"
rels_type_treatment_method = "TREATMENT_METHOD"


data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")


class DiseaseGraphExtractor(object):
    def __init__(self):

        self.global_identity = 1

        # 8 types of entities
        self.ent_disease = {}  # key is name, and value is detail of disease
        self.ent_symptom = {}
        self.ent_drug = {}
        self.ent_check = {}
        self.ent_department = {}
        self.ent_food = {}
        self.ent_recipe = {}
        self.ent_pharm_company = {}  # pharmaceutical company
        self.ent_treatment = {}

        # 12 types of relationships
        self.rels_department = []  # what department that a department belongs to, (department, belongs_to, department)
        self.rels_avoid_eat = []  # avoid eating certain food for disease, (disease, avoid_eat, food)
        self.rels_advise_eat = []  # advise eating certain food for disease, (disease, advise_eat, food)
        self.rels_recommend_recipe = []  # recommended recipe for disease, (disease, recommend_recipe, recipe)
        self.rels_common_drug = []  # commonly used drug for disease, (disease, common_drug, drug)
        self.rels_recommend_drug = []  # recommend drug for disease, (disease, recommend_drug, drug)
        self.rels_need_check = []  # check need to be taken for disease, (disease, need_check, check)
        self.rels_produce_drug = []  # drug the pharmaceutical company produce, (pharmaceutical company, produce_drug, drug)
        self.rels_complication = []  # complications of disease, (disease, has_complication, disease)
        self.rels_symptom = []  # symptom the disease has, (disease, has_symptom, symptom)
        self.rels_treat_department = []  # department to treat disease, (disease, treat_department, department)
        self.rels_treatment_method = []  # treatment methods for disease, (disease, treatment_method, treatment)

    def extract(self, file_path):
        """Extract triples from file and export to database or file in batches. """
        num_lines = sum([1 for _ in open(file_path, "r")])
        print("Extracting triples from file {} with {} diseases.".format(file_path, num_lines))
        with open(file_path, "r") as file:
            for no, line in tqdm(enumerate(file), total=num_lines):
                data = json.loads(line)
                disease = data.get("name")
                if not disease:
                    print("Error: The name of disease is not found for line {}!".format(no))
                    continue

                disease_dict = {
                    "name": disease,
                    "desc": data.get("desc"),
                    "prevent": data.get("prevent"),
                    "cause": data.get("cause"),
                    "susceptible_populations": data.get("easy_get"),
                    "treatment_cycle": data.get("cure_lasttime"),
                    "cure_rate": data.get("cured_prob"),
                    "treatment_cost": data.get("cost_money"),
                    "is_healthcare_disease": data.get("yibao_status"),
                    "prevalence_ratio": data.get("get_prob"),
                    "infection_mode": data.get("get_way")
                }

                if disease not in self.ent_disease:
                    disease_dict.update({"id": self.global_identity})
                    self.global_identity += 1
                    self.ent_disease[disease] = disease_dict

                if "recommand_drug" in data:
                    for drug in data["recommand_drug"]:
                        if drug not in self.ent_drug:
                            self.ent_drug[drug] = {"name": drug, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_recommend_drug.append((self.ent_disease[disease]["id"],
                                                         self.ent_drug[drug]["id"]))

                if "recommand_eat" in data:
                    for recipe in data["recommand_eat"]:
                        if recipe not in self.ent_recipe:
                            self.ent_recipe[recipe] = {"name": recipe, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_recommend_recipe.append((self.ent_disease[disease]["id"],
                                                           self.ent_recipe[recipe]["id"]))

                if "not_eat" in data:
                    for food in data["not_eat"]:
                        if food not in self.ent_food:
                            self.ent_food[food] = {"name": food, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_avoid_eat.append((self.ent_disease[disease]["id"],
                                                    self.ent_food[food]["id"]))

                if "do_eat" in data:
                    for food in data["do_eat"]:
                        if food not in self.ent_food:
                            self.ent_food[food] = {"name": food, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_advise_eat.append((self.ent_disease[disease]["id"],
                                                     self.ent_food[food]["id"]))

                if "symptom" in data:
                    for symptom in data["symptom"]:
                        if symptom not in self.ent_symptom:
                            self.ent_symptom[symptom] = {"name": symptom, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_symptom.append((self.ent_disease[disease]["id"],
                                                  self.ent_symptom[symptom]["id"]))

                if "acompany" in data:
                    for complication in data["acompany"]:
                        if complication not in self.ent_disease:
                            self.ent_disease[complication] = {"name": complication, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_complication.append((self.ent_disease[disease]["id"],
                                                       self.ent_disease[complication]["id"]))

                if "common_drug" in data:
                    for drug in data["common_drug"]:
                        if drug not in self.ent_drug:
                            self.ent_drug[drug] = {"name": drug, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_common_drug.append((self.ent_disease[disease]["id"],
                                                      self.ent_drug[drug]["id"]))

                if "check" in data:
                    for check in data["check"]:
                        if check not in self.ent_check:
                            self.ent_check[check] = {"name": check, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_need_check.append((self.ent_disease[disease]["id"],
                                                     self.ent_check[check]["id"]))

                if "cure_department" in data:
                    for dept in data["cure_department"]:
                        if dept not in self.ent_department:
                            self.ent_department[dept] = {"name": dept, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_treat_department.append((self.ent_disease[disease]["id"],
                                                           self.ent_department[dept]["id"]))

                    if len(data["cure_department"]) == 2:
                        small = data["cure_department"][1]
                        big = data["cure_department"][0]
                        self.rels_department.append((self.ent_department[small]["id"],
                                                     self.ent_department[big]["id"]))

                if "drug_detail" in data:
                    re_patn = re.compile(r"(?P<company>\w+)\((?P<drug>\w+)\)")
                    for item in data["drug_detail"]:
                        res = re_patn.match(item)
                        if res:
                            company = res.group("company")
                            drug = res.group("drug")
                            company = company.replace(drug, "")
                            if company not in self.ent_pharm_company:
                                self.ent_pharm_company[company] = {"name": company, "id": self.global_identity}
                                self.global_identity += 1
                            if drug not in self.ent_drug:
                                self.ent_drug[drug] = {"name": drug, "id": self.global_identity}
                                self.global_identity += 1
                            self.rels_produce_drug.append((self.ent_pharm_company[company]["id"],
                                                           self.ent_drug[drug]["id"]))

                if "cure_way" in data:
                    for method in data["cure_way"]:
                        if method not in self.ent_treatment:
                            self.ent_treatment[method] = {"name": method, "id": self.global_identity}
                            self.global_identity += 1
                        self.rels_treatment_method.append((self.ent_disease[disease]["id"],
                                                           self.ent_treatment[method]["id"]))

        self.rels_department = list(set(self.rels_department))
        self.rels_avoid_eat = list(set(self.rels_avoid_eat))
        self.rels_advise_eat = list(set(self.rels_advise_eat))
        self.rels_recommend_recipe = list(set(self.rels_recommend_recipe))
        self.rels_common_drug = list(set(self.rels_common_drug))
        self.rels_recommend_drug = list(set(self.rels_recommend_drug))
        self.rels_need_check = list(set(self.rels_need_check))
        self.rels_produce_drug = list(set(self.rels_produce_drug))
        self.rels_complication = list(set(self.rels_complication))
        self.rels_symptom = list(set(self.rels_symptom))
        self.rels_treat_department = list(set(self.rels_treat_department))
        self.rels_treatment_method = list(set(self.rels_treatment_method))

        num_entities = len(self.ent_disease) + len(self.ent_symptom) + len(self.ent_drug) + \
                       len(self.ent_check) + len(self.ent_department) + len(self.ent_food) + \
                       len(self.ent_recipe) + len(self.ent_pharm_company)
        num_rels = len(self.rels_department) + len(self.rels_avoid_eat) + len(self.rels_advise_eat) + \
                   len(self.rels_recommend_recipe) + len(self.rels_common_drug) + len(self.rels_recommend_drug) + \
                   len(self.rels_need_check) + len(self.rels_produce_drug) + len(self.rels_complication) + \
                   len(self.rels_symptom) + len(self.rels_treat_department) + len(self.rels_treatment_method)
        print("Has extracted {} entities and {} relationships.".format(num_entities, num_rels))

    def export_entities(self):
        entity_data_path = os.path.join(data_path, "entity")
        if not os.path.exists(entity_data_path):
            os.mkdir(entity_data_path)

        self.export_nodes_to_csv(self.ent_disease, entity_type_disease,
                                 os.path.join(entity_data_path, "disease.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_symptom, entity_type_symptom,
                                 os.path.join(entity_data_path, "symptom.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_drug, entity_type_drug,
                                 os.path.join(entity_data_path, "drug.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_check, entity_type_check,
                                 os.path.join(entity_data_path, "check.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_department, entity_type_department,
                                 os.path.join(entity_data_path, "department.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_food, entity_type_food,
                                 os.path.join(entity_data_path, "food.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_recipe, entity_type_recipe,
                                 os.path.join(entity_data_path, "recipe.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_pharm_company, entity_type_pharm_company,
                                 os.path.join(entity_data_path, "pharm_company.csv"),
                                 ["id"], ["ID"])
        self.export_nodes_to_csv(self.ent_treatment, entity_type_treatment,
                                 os.path.join(entity_data_path, "treatment.csv"),
                                 ["id"], ["ID"])

    def export_relationships(self):
        rels_data_path = os.path.join(data_path, "relationship")
        if not os.path.exists(rels_data_path):
            os.mkdir(rels_data_path)

        self.export_edges_to_csv(self.rels_department, rels_type_belongs_to,
                                 entity_type_department, entity_type_department,
                                 os.path.join(rels_data_path, "belongs_to.csv"))
        self.export_edges_to_csv(self.rels_avoid_eat, rels_type_avoid_eat,
                                 entity_type_disease, entity_type_food,
                                 os.path.join(rels_data_path, "avoid_eat.csv"))
        self.export_edges_to_csv(self.rels_advise_eat, rels_type_advise_eat,
                                 entity_type_disease, entity_type_food,
                                 os.path.join(rels_data_path, "advise_eat.csv"))
        self.export_edges_to_csv(self.rels_recommend_recipe, rels_type_recommend_recipe,
                                 entity_type_disease, entity_type_recipe,
                                 os.path.join(rels_data_path, "recommend_recipe.csv"))
        self.export_edges_to_csv(self.rels_common_drug, rels_type_common_drug,
                                 entity_type_disease, entity_type_drug,
                                 os.path.join(rels_data_path, "common_drug.csv"))
        self.export_edges_to_csv(self.rels_recommend_drug, rels_type_recommend_drug,
                                 entity_type_disease, entity_type_drug,
                                 os.path.join(rels_data_path, "recommend_drug.csv"))
        self.export_edges_to_csv(self.rels_need_check, rels_type_need_check,
                                 entity_type_disease, entity_type_check,
                                 os.path.join(rels_data_path, "need_check.csv"))
        self.export_edges_to_csv(self.rels_produce_drug, rels_type_produce_drug,
                                 entity_type_pharm_company, entity_type_drug,
                                 os.path.join(rels_data_path, "produce_drug.csv"))
        self.export_edges_to_csv(self.rels_complication, rels_type_has_complication,
                                 entity_type_disease, entity_type_disease,
                                 os.path.join(rels_data_path, "has_complication.csv"))
        self.export_edges_to_csv(self.rels_symptom, rels_type_has_symptom,
                                 entity_type_disease, entity_type_symptom,
                                 os.path.join(rels_data_path, "has_symptom.csv"))
        self.export_edges_to_csv(self.rels_treat_department, rels_type_treat_department,
                                 entity_type_disease, entity_type_department,
                                 os.path.join(rels_data_path, "treat_department.csv"))
        self.export_edges_to_csv(self.rels_treatment_method, rels_type_treatment_method,
                                 entity_type_disease, entity_type_treatment,
                                 os.path.join(rels_data_path, "treatment_method.csv"))

    def export_nodes_to_csv(self, nodes, node_label, file_name, properties=None, field_types=None):
        print("Exporting {} {} entities...".format(len(nodes), node_label))
        df = pd.DataFrame(nodes.values())
        if properties and field_types:
            columns = ["{}:{}".format(p, f) for p, f in zip(properties, field_types)]
            df.rename(columns=dict(zip(properties, columns)), inplace=True)
        df[":LABEL"] = [node_label] * len(nodes)
        df.to_csv(file_name, index=False, encoding="utf-8")

    def export_edges_to_csv(self, edges, edge_type, from_type, to_type, file_name):
        print("Exporting {} relationship ({}, {}, {})...".format(len(edges), edge_type,
                                                                 from_type, to_type))
        df = pd.DataFrame(edges, columns=[":START_ID", ":END_ID"])
        df[":TYPE"] = [edge_type] * len(edges)
        df.to_csv(file_name, index=False, encoding="utf-8")

    def export_as_dictionary(self):
        name_data_path = os.path.join(data_path, "name")
        if not os.path.exists(name_data_path):
            os.mkdir(name_data_path)

        self.export_names(self.ent_disease.keys(),
                          os.path.join(name_data_path, "disease.txt"))
        self.export_names(self.ent_symptom.keys(),
                          os.path.join(name_data_path, "symptom.txt"))
        self.export_names(self.ent_drug.keys(),
                          os.path.join(name_data_path, "drug.txt"))
        self.export_names(self.ent_check.keys(),
                          os.path.join(name_data_path, "check.txt"))
        self.export_names(self.ent_department.keys(),
                          os.path.join(name_data_path, "department.txt"))
        self.export_names(self.ent_food.keys(),
                          os.path.join(name_data_path, "food.txt"))
        self.export_names(self.ent_recipe.keys(),
                          os.path.join(name_data_path, "recipe.txt"))
        self.export_names(self.ent_pharm_company.keys(),
                          os.path.join(name_data_path, "pharm_company.txt"))
        self.export_names(self.ent_treatment.keys(),
                          os.path.join(name_data_path, "treatment.txt"))

    def export_names(self, names, file_name):
        with open(file_name, "w", encoding="utf-8") as file:
            for n in names:
                file.write(n + "\n")


if __name__ == '__main__':
    extractor = DiseaseGraphExtractor()
    extractor.extract("./data/medical.json")
    # extractor.export_entities()
    # extractor.export_relationships()
    extractor.export_as_dictionary()
