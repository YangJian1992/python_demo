from sklearn import datasets
from sklearn.cross_validation import train_test_split #分割训练集和测试集
from sklearn.preprocessing import StandardScaler #归一化
from sklearn import linear_model #线性模型，包括线性回归
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score#评估参数R方，越接近1，拟合得越好。也可以用.score(x_test, y_test)求r2
from sklearn import tree
import pydot
from sklearn.externals.six import StringIO
import os



#线性回归的模型
def linear_regression_example():
    #引用波士顿房价的数据样本
    scikit_boston = datasets.load_boston()
    x = scikit_boston.data
    y = scikit_boston.target

    # print(scikit_boston.feature_names)
    df = pd.DataFrame(data=np.c_[x,y], columns=np.append(scikit_boston.feature_names, ['MEDV']))
    # print(df.info())
    # print(df.describe())

    #归一化之前，先把样本划分为训练集和测试集，再进行归一化，不可直接对总样本进行归一化。
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)

    #归一化
    # scaler = StandardScaler()
    # x_train = scaler.fit_transform(x_train)
    # x_test = scaler.fit_transform(x_test)

    #训练模型。linear_model.LinearRegression(normalize=True)也可以归一化
    # df = pd.DataFrame(data=np.c_[x_train, y_train], columns=np.append(scikit_boston.feature_names, ['MEDV']))
    clf = linear_model.LinearRegression(normalize=True)
    clf.fit(x_train, y_train)

    #预测测试集
    y_pred = clf.predict(x_test)

    #r方评估
    r2 = r2_score(y_test, y_pred)
    r2 = clf.score(x_test, y_test)

    #自变量参数构成的矩阵w
    coef = clf.coef_



#决策树模型
def decision_tree_example():
    #使用鸢尾花样本
    iris = datasets.load_iris()
    x = iris.data
    y = iris.target
    print(x, y)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(x_train, y_train)
    y_pred = clf.predict(x_test)
    print(y_test, y_pred)
    r2 = clf.score(x_test, y_pred)
    print(r2)
    dot_data = StringIO()
    tree.export_graphviz(clf, out_file=dot_data)
    graph = pydot.graph_from_dot_data(dot_data.getvalue())
    graph.create_png("iris.pdf")

decision_tree_example()
