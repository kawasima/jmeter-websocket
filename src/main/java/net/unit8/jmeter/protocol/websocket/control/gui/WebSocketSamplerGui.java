package net.unit8.jmeter.protocol.websocket.control.gui;

import net.unit8.jmeter.protocol.websocket.sampler.WebSocketSampler;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.gui.util.HorizontalPanel;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.protocol.http.gui.HTTPArgumentsPanel;
import org.apache.jmeter.protocol.http.util.HTTPArgument;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import javax.swing.*;
import java.awt.*;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * GUI for WebSocetSampler
 *
 * @author kawasima
 */
public class WebSocketSamplerGui extends AbstractSamplerGui {
    private static final Logger log = LoggingManager.getLoggerForClass();

    private JTextField domain;
    private JTextField port;
    private JTextField protocol;
    private JTextField contentEncoding;
    private JTextField path;
    private JTextArea  sendMessage;
    private JTextArea  recvMessage;
    private HTTPArgumentsPanel argsPanel;

    private boolean displayName = true;
    private static final ResourceBundle resources;

    static {
        Locale loc = JMeterUtils.getLocale();
        resources = ResourceBundle.getBundle(WebSocketSampler.class.getName() + "Resources", loc);
        log.info("Resource " + WebSocketSampler.class.getName() +  //$NON-NLS-1$
                " is loaded for locale " + loc); //$NON-NLS-1$
    }

    public WebSocketSamplerGui() {
        this(true);
    }

    public WebSocketSamplerGui(boolean displayName) {
        this.displayName = displayName;
        init();
    }

    @Override
    public String getLabelResource() {
        throw new IllegalStateException("This shouldn't be called"); //$NON-NLS-1$
    }

    @Override
    public String getStaticLabel() {
        return getResString("websocket_testing_title");  //$NON-NLS-1$
    }
    @Override
    public void configure(TestElement element) {
        super.configure(element);
        domain.setText(element.getPropertyAsString(WebSocketSampler.DOMAIN));
        port.setText(element.getPropertyAsString(WebSocketSampler.PORT));
        protocol.setText(element.getPropertyAsString(WebSocketSampler.PROTOCOL));
        path.setText(element.getPropertyAsString(WebSocketSampler.PATH));
        contentEncoding.setText(element.getPropertyAsString(WebSocketSampler.CONTENT_ENCODING));

        Arguments arguments = (Arguments) element.getProperty(WebSocketSampler.ARGUMENTS).getObjectValue();
        argsPanel.configure(arguments);

        sendMessage.setText(element.getPropertyAsString(WebSocketSampler.SEND_MESSAGE));
        recvMessage.setText(element.getPropertyAsString(WebSocketSampler.RECV_MESSAGE));
    }

    @Override
    public TestElement createTestElement() {
        WebSocketSampler element = new WebSocketSampler();

        element.setName(getName());
        element.setProperty(TestElement.GUI_CLASS, this.getClass().getName());
        element.setProperty(TestElement.TEST_CLASS, element.getClass().getName());

        modifyTestElement(element);
        return element;
    }

    @Override
    public void modifyTestElement(TestElement element) {
        configureTestElement(element);
        element.setProperty(WebSocketSampler.DOMAIN, domain.getText());
        element.setProperty(WebSocketSampler.PATH, path.getText());
        element.setProperty(WebSocketSampler.PORT, port.getText());
        element.setProperty(WebSocketSampler.PROTOCOL, protocol.getText());
        element.setProperty(WebSocketSampler.CONTENT_ENCODING, contentEncoding.getText());

        Arguments args = (Arguments) argsPanel.createTestElement();
        HTTPArgument.convertArgumentsToHTTP(args);
        element.setProperty(new TestElementProperty(WebSocketSampler.ARGUMENTS, args));

        element.setProperty(WebSocketSampler.SEND_MESSAGE, sendMessage.getText());
        element.setProperty(WebSocketSampler.RECV_MESSAGE, recvMessage.getText());
    }

    private JPanel getDomainPanel() {
        domain = new JTextField(20);

        JLabel label = new JLabel(JMeterUtils.getResString("web_server_domain")); // $NON-NLS-1$
        label.setLabelFor(domain);

        JPanel panel = new JPanel(new BorderLayout(5, 0));
        panel.add(label, BorderLayout.WEST);
        panel.add(domain, BorderLayout.CENTER);
        return panel;
    }

    private JPanel getPortPanel() {
        port = new JTextField(4);

        JLabel label = new JLabel(JMeterUtils.getResString("web_server_port")); // $NON-NLS-1$
        label.setLabelFor(port);

        JPanel panel = new JPanel(new BorderLayout(5, 0));
        panel.add(label, BorderLayout.WEST);
        panel.add(port, BorderLayout.CENTER);

        return panel;
    }

    protected Component getProtocolAndPathPanel() {
        // PATH
        path = new JTextField(15);
        JLabel pathLabel = new JLabel(JMeterUtils.getResString("path")); //$NON-NLS-1$
        pathLabel.setLabelFor(path);

        // PROTOCOL
        protocol = new JTextField(4);
        JLabel protocolLabel = new JLabel(JMeterUtils.getResString("protocol")); // $NON-NLS-1$
        protocolLabel.setLabelFor(protocol);

        // CONTENT_ENCODING
        contentEncoding = new JTextField(10);
        JLabel contentEncodingLabel = new JLabel(JMeterUtils.getResString("content_encoding")); // $NON-NLS-1$
        contentEncodingLabel.setLabelFor(contentEncoding);

        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        panel.add(pathLabel);
        panel.add(path);
        panel.add(Box.createHorizontalStrut(5));

        panel.add(protocolLabel);
        panel.add(protocol);
        panel.add(Box.createHorizontalStrut(5));

        panel.add(contentEncodingLabel);
        panel.add(contentEncoding);
        panel.setMinimumSize(panel.getPreferredSize());

        return panel;
    }

    private JPanel getSendMessagePanel() {
        JLabel sendMessageLabel = new JLabel(getResString("websocket_send_message")); // $NON-NLS-1$
        sendMessage = new JTextArea(3, 0);
        sendMessage.setLineWrap(true);
        sendMessageLabel.setLabelFor(sendMessage);

        JPanel sendMessagePanel = new JPanel(new BorderLayout(5, 0));
        sendMessagePanel.add(sendMessageLabel, BorderLayout.WEST);
        sendMessagePanel.add(sendMessage, BorderLayout.CENTER);
        return sendMessagePanel;
    }

    private JPanel getRecvMessagePanel() {
        JLabel recvMessageLabel = new JLabel(getResString("websocket_recv_message")); // $NON-NLS-1$
        recvMessage = new JTextArea(3, 0);
        recvMessage.setLineWrap(true);
        recvMessageLabel.setLabelFor(recvMessage);

        JPanel recvMessagePanel = new JPanel(new BorderLayout(5, 0));
        recvMessagePanel.add(recvMessageLabel, BorderLayout.WEST);
        recvMessagePanel.add(recvMessage, BorderLayout.CENTER);
        return recvMessagePanel;
    }

    private void init() {
        setLayout(new BorderLayout(0, 5));

        if (displayName) {
            setBorder(makeBorder());
            add(makeTitlePanel(), BorderLayout.NORTH);
        }

        // MAIN PANEL
        VerticalPanel mainPanel = new VerticalPanel();
        JPanel webRequestPanel = new HorizontalPanel();
        JPanel serverPanel = new JPanel();
        serverPanel.setLayout(new BoxLayout(serverPanel, BoxLayout.X_AXIS));
        serverPanel.add(getDomainPanel());
        serverPanel.add(getPortPanel());

        webRequestPanel.add(serverPanel, BorderLayout.NORTH);
        JPanel northPanel = new JPanel();
        northPanel.setLayout(new BoxLayout(northPanel, BoxLayout.Y_AXIS));
        northPanel.add(getProtocolAndPathPanel());

        webRequestPanel.add(northPanel, BorderLayout.CENTER);
        argsPanel = new HTTPArgumentsPanel();
        webRequestPanel.add(argsPanel, BorderLayout.SOUTH);

        mainPanel.add(webRequestPanel);
        mainPanel.add(getSendMessagePanel());
        mainPanel.add(getRecvMessagePanel());
        add(mainPanel, BorderLayout.CENTER);
    }

    /**
     * Gets the resource string for this key.
     *
     * If the resource is not found, a warning is logged
     *
     * @param key
     *            the key in the resource file
     * @return the resource string if the key is found; otherwise, return
     *         "[res_key="+key+"]"
     */
    public static String getResString(String key) {
        return getResStringDefault(key, RES_KEY_PFX + key + "]"); //$NON-NLS-1$
    }

    public static final String RES_KEY_PFX = "[res_key="; //$NON-NLS-1$

    /*
     * Helper method to do the actual work of fetching resources; allows
     * getResString(S,S) to be deprecated without affecting getResString(S);
     */
    private static String getResStringDefault(String key, String defaultValue) {
        if (key == null) {
            return null;
        }
        // Resource keys cannot contain spaces
        key = key.replace(' ', '_'); // $NON-NLS-1$ // $NON-NLS-2$
        key = key.toLowerCase(java.util.Locale.ENGLISH);
        String resString = null;
        try {
            resString = resources.getString(key);
        } catch (MissingResourceException mre) {
            log.warn("ERROR! Resource string not found: [" +  //$NON-NLS-1$
                    key + "]", mre); //$NON-NLS-1$
            resString = defaultValue;
        }
        return resString;
    }
}
