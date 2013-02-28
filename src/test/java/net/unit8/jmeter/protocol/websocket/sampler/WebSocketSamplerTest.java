package net.unit8.jmeter.protocol.websocket.sampler;

import net.unit8.jmeter.protocol.websocket.sampler.WebSocketSampler;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.CSVDataSet;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.control.OnceOnlyController;
import org.apache.jmeter.control.ReplaceableController;
import org.apache.jmeter.control.gui.TestPlanGui;
import org.apache.jmeter.engine.JMeterEngine;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.gui.tree.JMeterTreeModel;
import org.apache.jmeter.gui.tree.JMeterTreeNode;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.testelement.property.BooleanProperty;
import org.apache.jmeter.testelement.property.StringProperty;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.timers.UniformRandomTimer;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;
import org.apache.jorphan.collections.ListedHashTree;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.util.HeapDumper;
import org.apache.log.Logger;

import javax.xml.transform.Result;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * Tests of WebSocketSampler
 * 
 * @author kawasima
 */
public class WebSocketSamplerTest {
    private static final Logger log = LoggingManager.getLoggerForClass();

    public static void main(String[] args) throws Exception {
        JMeterUtils.setJMeterHome("src/test/resources/");
        JMeterUtils.loadJMeterProperties("src/test/resources/jmeter.properties");
        JMeterUtils.setProperty("saveservice_properties", "saveservice.properties");
        JMeterUtils.setProperty("search_paths", "ApacheJMeter_functions-2.9.jar");
        JMeterUtils.setLocale(Locale.JAPAN);
        
        JMeterEngine engine = new StandardJMeterEngine();
        HashTree config = new ListedHashTree();
        TestPlan testPlan = new TestPlan("websocket test");
        testPlan.setFunctionalMode(false);
        testPlan.setSerialized(false);
        testPlan.setProperty(new BooleanProperty(TestElement.ENABLED, true));
        testPlan.setUserDefinedVariables(new Arguments());

        ThreadGroup threadGroup = new ThreadGroup();
        threadGroup.setNumThreads(300);
        threadGroup.setRampUp(20);
        threadGroup.setDelay(0);
        threadGroup.setDuration(0);
        threadGroup.setProperty(new StringProperty(ThreadGroup.ON_SAMPLE_ERROR, "continue"));
        threadGroup.setScheduler(false);
        threadGroup.setName("Group1");
        threadGroup.setProperty(new BooleanProperty(TestElement.ENABLED, true));

        LoopController controller = new LoopController();
        controller.setLoops(10);
        controller.setContinueForever(false);
        controller.setProperty(new BooleanProperty(TestElement.ENABLED, true));
        threadGroup.setProperty(new TestElementProperty(ThreadGroup.MAIN_CONTROLLER, controller));

        CSVDataSet csvDataSet = new CSVDataSet();
        csvDataSet.setProperty(new StringProperty("filename", "src/test/resources/users.csv"));
        csvDataSet.setProperty(new StringProperty("variableNames", "USER_NAME"));
        csvDataSet.setProperty(new StringProperty("delimiter", ","));
        csvDataSet.setProperty(new StringProperty("shareMode", "shareMode.all"));
        csvDataSet.setProperty("quoted", false);
        csvDataSet.setProperty("recycle", true);
        csvDataSet.setProperty("stopThread", false);

        WebSocketSampler sampler = new WebSocketSampler();
        sampler.setName("WebSocket Test");
        sampler.setProperty(new BooleanProperty(TestElement.ENABLED, true));
        sampler.addNonEncodedArgument("name", "${USER_NAME}", "=");
        sampler.setContentEncoding("UTF-8");
        sampler.setProtocol("ws");
        sampler.setDomain("localhost");
        sampler.setPort(9090);
        sampler.setPath("/", "UTF-8");
        sampler.setSendMessage("${__RandomString(50,ABCDEFGHIJKLMNOPQRSTUVWXYZ)}");
        sampler.setRecvMessage("\"name\":\"${USER_NAME}\"");

        OnceOnlyController onceOnlyController = new OnceOnlyController();

        Summariser summariser = new Summariser();

        HashTree tpConfig = config.add(testPlan);
        HashTree tgConfig = tpConfig.add(threadGroup);
        HashTree oocConfig = tgConfig.add(onceOnlyController);
        oocConfig.add(csvDataSet);

        UniformRandomTimer randomTimer = new UniformRandomTimer();
        randomTimer.setRange(3000);
        HashTree samplerConfig = tgConfig.add(sampler);
        samplerConfig.add(summariser);
        tgConfig.add(randomTimer);

        engine.configure(config);
        engine.runTest();
    }
}
