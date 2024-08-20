from typing import Optional

import torch
import torch.nn.functional as F
from torch import Tensor
from torch.nn import BatchNorm1d, Linear, ReLU, Sequential

from torch_geometric.nn import GINConv, GCNConv, global_mean_pool

# from torch_geometric.nn import GCNConv

class GCN(torch.nn.Module):
    def __init__(self, in_channels=32, hidden_channels=16, n_classes=8):
        super().__init__()
        self.conv1 = GCNConv(in_channels, hidden_channels)
        # self.conv1 = conv1.jittable('(Tensor, Tensor) -> Tensor')
        self.conv2 = GCNConv(hidden_channels, n_classes)
        # self.conv2 = conv2.jittable('(Tensor, Tensor) -> Tensor')

    def forward(self, x, edge_index):
        # x, edge_index = data.x, data.edge_index

        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, training=self.training)
        x = self.conv2(x, edge_index)

        return F.log_softmax(x, dim=1)
    
class GIN(torch.nn.Module):
    def __init__(self, in_channels: int, hidden_channels: int,
                 out_channels: int, num_layers: int):
        super().__init__()

        self.convs = torch.nn.ModuleList()
        for _ in range(num_layers):
            mlp = Sequential(
                Linear(in_channels, hidden_channels),
                BatchNorm1d(hidden_channels),
                ReLU(),
                Linear(hidden_channels, hidden_channels),
            )
            self.convs.append(GINConv(mlp))
            # self.convs.append(GCNConv(mlp))
            in_channels = hidden_channels

        self.lin1 = Linear(hidden_channels, hidden_channels)
        self.lin2 = Linear(hidden_channels, out_channels)

    def forward(
        self,
        x: Tensor,
        edge_index: Tensor,
        batch: Optional[Tensor] = None,
    ) -> Tensor:
        for conv in self.convs:
            x = conv(x, edge_index).relu()

        x = global_mean_pool(x, batch)

        x = self.lin1(x).relu()
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.lin2(x)

        return x


model = GIN(32, 64, 16, num_layers=3)
# model = GCN(32, 16, 4)
model = torch.jit.script(model)
model.save('model.pt')
