"""
@author: Qinjuan Yang
@time: 2022-03-28 23:41
@desc: 
"""


class MedicalBot(object):
    def __init__(self):
        self.semantic_parser = None

    def answer(self, question):
        res = self.semantic_parser.parse(question)
        # ans = get_answer(res)
        return None

