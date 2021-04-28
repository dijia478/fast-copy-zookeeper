import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * zookeeper复制工具(高级api)
 *
 * @author dijia478
 * @date 2021-4-27 23:35:53
 */
public class FastCopyZookeeper {

    /**
     * 重试策略
     */
    private static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

    /**
     * 根节点目录
     */
    private static final String ROOT_PATH = "/rkhd";

    /**
     * 来源zk客户端
     */
    private static CuratorFramework sourceZk;

    /**
     * 目标zk客户端
     */
    private static CuratorFramework targetZk;

    /**
     * 来源zk地址
     */
    private static String sourceZkAddress;

    /**
     * 目标zk地址
     */
    private static String targetZkAddress;

    /**
     * 处理节点统计
     */
    private static final AtomicInteger COUNT = new AtomicInteger();

    /**
     * 工具执行开始方法
     *
     * @param args 工具参数，目前写死的地址，不传也可以
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long now = System.currentTimeMillis();

        setAddress(args);
        connectionZk();

        System.out.println("开始删除targetZk：" + targetZkAddress);
        deleteTargetZk(ROOT_PATH);
        System.out.println("targetZk：" + targetZkAddress + "删除完成");

        System.out.println("开始复制targetZk：" + targetZkAddress);
        copyTargetZk(ROOT_PATH);
        System.out.println("数据复制完成，共耗时：" + (System.currentTimeMillis() - now) / 1000 + "秒, 处理" + COUNT.get() + "个节点");
    }

    /**
     * zk地址设置
     *
     * @param args 工具参数，目前写死的地址，不传也可以
     */
    private static void setAddress(String[] args) {
        sourceZkAddress = "192.168.0.144:2181";
        // targetZkAddress = args[0];
        targetZkAddress = "127.0.0.1:2181";
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "256");
    }

    /**
     * 将数据复制到目标zk上
     *
     * @param nodePath 节点路径
     * @throws Exception
     */
    private static void copyTargetZk(String nodePath) throws Exception {
        List<String> list = sourceZk.getChildren().forPath(nodePath);
        list.parallelStream().forEach(str -> {
            try {
                String childNodePath = nodePath + "/" + str;
                copyTargetZk(childNodePath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        if (list.isEmpty()) {
            byte[] data = sourceZk.getData().forPath(nodePath);
            if (targetZk.checkExists().forPath(nodePath) != null) {
                targetZk.setData()
                        .withVersion(-1)
                        .forPath(nodePath, data);
            } else {
                targetZk.create()
                        // 递归创建所需父节点
                        .creatingParentContainersIfNeeded()
                        // 创建类型为持久节点
                        .withMode(CreateMode.PERSISTENT)
                        // 目录及内容
                        .forPath(nodePath, data);
            }
            COUNT.incrementAndGet();
        }
    }

    /**
     * 删除目标zk的数据
     *
     * @param nodePath 节点路径
     * @throws Exception
     */
    private static void deleteTargetZk(String nodePath) throws Exception {
        List<String> list = targetZk.getChildren().forPath(nodePath);
        list.parallelStream().forEach(str -> {
            try {
                String childNodePath = nodePath + "/" + str;
                deleteTargetZk(childNodePath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        targetZk.delete()
                // 强制保证删除
                .guaranteed()
                .deletingChildrenIfNeeded()
                // 指定删除的版本号
                .withVersion(-1)
                .forPath(nodePath);
    }

    /**
     * 连接源zk和目标zk
     */
    private static void connectionZk() {
        System.out.println("开始连接zookeeper");
        if (sourceZk == null) {
            sourceZk = CuratorFrameworkFactory.builder().connectString(sourceZkAddress)
                    // 会话超时时间
                    .sessionTimeoutMs(5000)
                    // 连接超时时间
                    .connectionTimeoutMs(5000)
                    // 重试策略
                    .retryPolicy(RETRY_POLICY)
                    .build();
            sourceZk.start();
            System.out.println("sourceZk：" + sourceZkAddress + "连接成功");
        }
        if (targetZk == null) {
            targetZk = CuratorFrameworkFactory.builder().connectString(targetZkAddress)
                    // 会话超时时间
                    .sessionTimeoutMs(5000)
                    // 连接超时时间
                    .connectionTimeoutMs(5000)
                    // 重试策略
                    .retryPolicy(RETRY_POLICY)
                    .build();
            targetZk.start();
            System.out.println("targetZk：" + targetZkAddress + "连接成功");
        }
    }

}