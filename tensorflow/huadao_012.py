import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
import pandas as pd
from pandas import DataFrame, Series
import numpy as np
import random


INPUT_NODE = 24 # 输入变量的个数
OUTPUT_NODE = 3 # 输出节点数量
LAYER1_NODE = 30 # 第一个隐藏层，节点数量为50
LAYER2_NODE = 10 #第二个隐藏层，节点数量
BATCH_SIZE = 100 # batch的大小
LEARNING_RATE_BASE = 0.8 # 基础的学习率
LEARNING_RATE_DECAY = 0.99 # 学习率的衰减率
REGULARIZATION_RATE = 0.0001 # 描述模型复杂度的正则化项在损失函数中的系数，lambda
TRAINING_STEPS = 30000 # 训练轮数
MOVING_AVERAGE_DECAY = 0.99 #滑动平均衰减率

def inference(input_tensor, avg_class, weights1, biases1, weights2, biases2, weights3, biases3):
    '''
    :param input_tensor: 输入值
    :param avg_calss:滑动平均
    :param weights1:
    :param biases1:
    :param weights2:
    :param biases2:
    :return: 计算前向传播结果
    '''
    #当没有提供滑动平均类时， 直接使用参数当前的取值
    if avg_class == None:
        #隐藏层的的前向传播结果， 使用了ReLU函数
        layer1 = tf.nn.relu(tf.matmul(input_tensor, weights1) + biases1)
        layer2 = tf.nn.relu(tf.matmul(layer1, weights2) + biases2)
        #计算输出层的前向传播结果。由于在计算损失函数时会一并计算softmax函数，所以这里不需要激活函数。
        return(tf.matmul(layer2, weights3) + biases3)
    else:
        # 首先使用avg_class.average函数来计算得出变量的滑动平均值
        # 然后计算相应的神经网络前各传播结果
        layer1 = tf.nn.relu(tf.matmul(input_tensor, avg_class.average(weights1)) + avg_class.average(biases1))
        layer2 = tf.nn.relu(tf.matmul(layer1, avg_class.average(weights2)) + avg_class.average(biases2))
        return (tf.matmul(layer2, avg_class.average(weights3)) + avg_class.average(biases3))

def train(data):
    # data共25个字段，前24个字段是输入，最后一个字段"label"是标签值
    train_data = np.array(data.iloc[:1600, :-1])
    train_y = np.array(list(data.iloc[:1600, -1]))

    validation_data = np.array(data.iloc[1500:1600, :-1])
    print(validation_data.shape)
    validation_y = np.array(list(data.iloc[1500:1600, -1]))
    print(validation_y.shape)

    test_data = np.array(data.iloc[1550:1619, :-1])
    test_y = np.array(list(data.iloc[1550:1619, -1]))

    x = tf.placeholder(tf.float32, [None, INPUT_NODE], name='x-input')
    y_ = tf.placeholder(tf.float32, [None, OUTPUT_NODE], name='y-input')

    #生成隐藏层1的参数
    weights1 = tf.Variable( tf.truncated_normal([INPUT_NODE, LAYER1_NODE], stddev=0.1))
    biases1 = tf.Variable(tf.constant(.1, shape=[LAYER1_NODE]))

    #生成隐藏层2的参数
    weights2 = tf.Variable( tf.truncated_normal([LAYER1_NODE, LAYER2_NODE], stddev=0.1))
    biases2 = tf.Variable(tf.constant(.1, shape=[LAYER2_NODE]))

    #生成输出层的参数
    weights3 = tf.Variable(tf.truncated_normal([LAYER2_NODE, OUTPUT_NODE], stddev=.1))
    biases3 = tf.Variable(tf.constant(.1, shape=[OUTPUT_NODE]))

    # 计算在当前参数下神经网络前向传播的结果。这里给出的用于计算滑动平均类为None, 所以函数不会使用参数的滑动平均值。
    y = inference(x, None, weights1, biases1, weights2, biases2, weights3, biases3)
    # 定义存储训练轮数的变量。为不可训练变量（trainable=False）， 不需要计算滑动平均值。
    global_step = tf.Variable(0, trainable=False)

    #给定滑动平均衰减率和训练轮数的变量，初始化滑动平均类。定训练轮数的变量可以加快训练早期变量的更新速度。
    variable_averages = tf.train.ExponentialMovingAverage(MOVING_AVERAGE_DECAY, global_step)

    # 在所有代表神经网络参数的变量上使用滑动平均。其他辅助变量（global_step）就不需要了。tf.trainable_variables返回的就是图上集合GraphKeys.TRAINABLE_VARIABLES
    # 中的元素。这个集合中元素就是所有没有指定trainable=False的参数。
    variable_averages_op = variable_averages.apply(tf.trainable_variables())

    # 计算使用了滑动平均之后的前向传播结果。滑动平均不会改变变量本身的取值， 而是会维护一个影子变量来记录其滑动平均值。所以当需要使用这个滑动平均值时，需要明确调用average函数
    average_y = inference(x, variable_averages, weights1, biases1, weights2, biases2, weights3, biases3)

    # 计算交叉熵作为刻画预测值和真实值之间差距的损失函数。当分类问题只有一个正确答案时，可以使用sparse_softmax_cross_entropy_with_logits函数，这个函数
    # 的第一个参数是不包括softmax层的前向传播结果， 第二个是训练数据的正确答案，所以要使用tf.argmax函数来得到对应的类别编号。
    cross_entropy = tf.nn.sparse_softmax_cross_entropy_with_logits(logits=y, labels=tf.argmax(y_, 1))

    #计算在当前batch中所有样例的交叉熵平均值
    cross_entropy_mean = tf.reduce_mean(cross_entropy)

    # 计算L2正则化损失函数.REGULARIZATION_RATE 就是正则化项的权重lambda
    regularizer = tf.contrib.layers.l2_regularizer(REGULARIZATION_RATE)
    # 计算模型的正则化损失。一般只计算神经网络边上权重的正则化损失，而不使用偏置项。
    regularization = regularizer(weights1) + regularizer(weights2) + regularizer(weights3)
    # 总损失等于交叉熵损失和正则化损失的和
    loss = cross_entropy_mean + regularization

    # 设置指数衰减的学习率
    learning_rate = tf.train.exponential_decay(
        LEARNING_RATE_BASE, #基础的学习率， 随着迭代的进行， 更新变量时使用的学习率在这个基础上递减
        global_step, # 当前迭代的轮数
        len(train_data) / BATCH_SIZE, # 过完所有的训练数据需要的迭代次数
        LEARNING_RATE_DECAY # 学习率的衰减速度
    )

    #使用tf.train.GradientDescentOptimizer优化算法来优化损失函数。这里的损失函数包括了交叉熵和L2正则化损失
    train_step = tf.train.GradientDescentOptimizer(learning_rate).minimize(loss, global_step=global_step)

    # 在训练神经网络模型时， 每过一遍数据既需要通过反向传播来更新神经网络中的参数， 又要更新第一个参数的滑动平均值。为了一次完成多个操作，tensorflow
    # 提供了tf.control_dependencies和tf.group两种机制。下面的程序都是可以的。
    # train_op = tf.group(train_step, variable_averages_op)
    with tf.control_dependencies([train_step, variable_averages_op]):
        train_op = tf.no_op(name='train')
    '''检验使用了滑动平均模型的神经网络前向传播结果是否正确。tf.argmax(average_y, 1)计算每一个样例的预测答案。其中average_y是一个batch_size*10
    的二维数组，每一行表示一个样例的前向传播结果。tf.argmax()的第二个参数“1”表示选取最大值的操作仅在第一个维度中进行， 也就是说， 只在每一行选取最大值
    对应的下标。于是得到的结果是一个长度为batch的一维数组， 这个一维数组中值就表示了每一个样例对应的数字识别结果。tf.equal判断两个张量的每一维是
    否相等，若相等则返回True. 这里的参数“1” 是指行， ”0“是指列。每一行中概率最大值就是预测结果，相对应的序号就是对应的数字。
    如：[0.8, 0.1,0.1, 0,....]， 最大值0.8对应的序号 "0" 就是识别的数字类别0.'''
    correct_prediction = tf.equal(tf.argmax(average_y, 1), tf.argmax(y_, 1))

    # 布尔型的数值转换为实数型，再计算平均值。tf.cast可以转换类型的。
    accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
    # saver = tf.train.Saver()#持久化模型
    #初始化会话并开始训练过程
    with tf.Session() as sess:
        tf.global_variables_initializer().run()
        #准备验证数据。一般在神经网络的训练过程中会通过验证数据来大致判断停止的条件和评判训练的效果。
        validate_feed = {x:validation_data, y_:validation_y}

        # 准备测试数据。在实际应用中， 这部分数据在训练时是不可见的， 这个数据只是作为模型优劣的最后评价标准。
        test_feed = {x:test_data, y_:test_y}

        #迭代地训练神经网络
        for i in range(TRAINING_STEPS):
            #产生这一轮使用的一个batch的训练数据，并运行训练过程。
            # 在0到2769之间产生100个不重复的整数, 得到batch数据
            batch_index = random.sample(range(0, 1619), BATCH_SIZE)
            xs = np.array(data.iloc[batch_index, :-1])
            ys = np.array(list(data.iloc[batch_index, -1]))
            # 每1000轮输出一次在验证数据集上的测试结果
            if i % 1000 == 0:
            # 计算滑动平均模型在验证数据上的结果。当模型复杂时，或验证数据上比较大时，太大的batch会导致计算时间过长甚至发生内在溢出的错误。
                validate_acc = sess.run(accuracy, feed_dict=validate_feed)
                print('在 %d 次训练后， 使用average_model 验证正确率是 %g'%(i, validate_acc))# %g:指数(e)或浮点数 (根据显示长度)

            sess.run(train_op, feed_dict={x:xs, y_:ys})

        #在训练结束之后， 在测试数据上检测神经网络模型的最终正确率
        test_acc = sess.run(accuracy, feed_dict=test_feed)
        saver.save(sess, './model.ckpt')
        print('在 %d 次训练之后， 使用average model的测试正确率是 %g' %(TRAINING_STEPS, test_acc))
        result = sess.run(tf.argmax(average_y, 1), feed_dict={x : test_data})
        # Series(result).to_csv('D:\\work\\华道征信-测试数据\\信贷详情\\test_result.csv',sep=',', encoding='gbk')
        Series(result).to_csv('D:\\work\\dian_hua_bang\\2018-4-10\\test_result.csv', sep=',', encoding='gbk')

def read_data():
    # path = 'D:\\work\\华道征信-测试数据\\信贷详情\\'
    # data = pd.read_excel(path+'信贷详情1.xlsx', sheetname='汇总', header=0)
    # co_list = [item for item in data.columns if ('非' in item)]
    # co_list.append('label')
    # data_2 = data.loc[:, co_list]
    # # 三分类问题，对应数字0,1,2， 如:第0类对应概率列表[1, 0, 0], 第1类对应列表[0, 1, 0]...
    # data_2['label'] = data_2['label'].map(lambda x: [ 1 if i==x else j for i, j in enumerate([0, 0, 0])])
    # return data_2
    path = 'D:\\work\\dian_hua_bang\\2018-4-10\\'
    data = pd.read_excel(path + 'cuishoufen.xlsx', sheetname='Sheet3', header=0)
    test = data.iloc[:, 3:].apply(lambda x: (np.array(x) - np.mean(np.array(x)))/np.std(np.array(x)))
    test.insert(24, 'label', data['label'])
    test['label'] = test['label'].map(lambda x: [1 if i == x else j for i, j in enumerate([0, 0, 0])])
    return test


#主程序入口
def main(argv=None):
    #声明处理MNIST数据集的类， 这个类在初始化时会自动下载数据。
    data = read_data()
    train(data)



# tf.app.run 会调用上面定义的main函数
if __name__ == '__main__':
    tf.app.run()





