"""
@author: Qinjuan Yang
@time: 2022-04-01 22:36
@desc: 
"""
from torch import nn
import torch
from transformers import BertModel, BertForSequenceClassification


class TextCNN(nn.Module):
    def __init__(self,
                 embedding_dim,
                 num_classes,
                 max_length,
                 kernel_size=(3, 4, 5),
                 num_filers=100,
                 dropout=0.3):
        super(TextCNN, self).__init__()
        self.convs = nn.ModuleList([
            nn.Sequential(nn.Conv2d(1, num_filers, (ks, embedding_dim)),
                          nn.ReLU(),
                          nn.MaxPool2d((max_length - ks + 1, 1)))
            for ks in kernel_size
        ])
        self.fc = nn.Linear(num_filers * len(kernel_size), num_classes)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x = x.unsqueeze(1)  # (batch, 1, seq_len, embedding_dim)
        out = [conv(x) for conv in self.convs]  # each is (batch, num_filters, 1, 1)
        out = torch.cat(out, dim=1)  # (batch, num_filers * len(kernel_size), 1, 1)
        out = out.squeeze()  # (batch, num_filters * len(kernel_size))
        out = self.dropout(out)  # (batch, num_filters * len(kernel_size))
        logits = self.fc(out)  # (batch, num_classes)
        return logits


class BertTextCNN(nn.Module):
    def __init__(self, model_path):
        super(BertTextCNN, self).__init__()
        pass

    def forward(self, x):
        pass
    