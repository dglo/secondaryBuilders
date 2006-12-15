/**
 * Class: SBComponent
 *
 * Date: Nov 28, 2006 2:27:09 PM
 *
 * (c) 2005 IceCube collaboration
 */
package icecube.daq.secBuilder;

import icecube.daq.juggler.component.DAQComponent;
import icecube.daq.juggler.component.DAQConnector;
import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.juggler.component.DAQCompServer;
import icecube.daq.payload.*;
import icecube.daq.common.DAQCmdInterface;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.SplicerImpl;
import icecube.daq.io.Dispatcher;
import icecube.daq.io.FileDispatcher;
import icecube.daq.io.SpliceablePayloadInputEngine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is the place where we initialize all the IO engines, splicers
 * and monitoring classes for secondary builders
 *
 * @author artur
 * @version $Id: SBComponent.java,v 1.0 2006/11/28 14:27:09 artur Exp $
 */
public class SBComponent extends DAQComponent {

    private Log log = LogFactory.getLog(SBComponent.class);

    private IByteBufferCache tcalBufferCache;
    private IByteBufferCache snBufferCache;
    private IByteBufferCache moniBufferCache;

    private SBCompConfig compConfig;

    private SpliceableFactory tcalFactory;
    private SpliceableFactory snFactory;
    private SpliceableFactory moniFactory;

    private Splicer tcalSplicer;
    private Splicer snSplicer;
    private Splicer moniSplicer;

    private SBSplicedAnalysis tcalSplicedAnalysis;
    private SBSplicedAnalysis snSplicedAnalysis;
    private SBSplicedAnalysis moniSplicedAnalysis;

    private Dispatcher tcalDispatcher;
    private Dispatcher snDispatcher;
    private Dispatcher moniDispatcher;

    private SecBuilderMonitor tcalBuilderMonitor;
    private SecBuilderMonitor snBuilderMonitor;
    private SecBuilderMonitor moniBuilderMonitor;

    private SpliceablePayloadInputEngine tcalInputEngine;
    private SpliceablePayloadInputEngine snInputEngine;
    private SpliceablePayloadInputEngine moniInputEngine;

    private static final String COMP_NAME = DAQCmdInterface.DAQ_SECONDARY_BUILDERS;
    private static final int COMP_ID = 0;

    public SBComponent(SBCompConfig compConfig) {
        super(COMP_NAME, COMP_ID);
        this.compConfig = compConfig;

        // init the parameters for ByteBufferCache
        int granularity = compConfig.getGranularity();
        long maxCacheBytes = compConfig.getMaxCacheBytes();
        long maxAcquireBytes = compConfig.getMaxAcquireBytes();

        // init tcalBuilder classes
        if (compConfig.isTcalEnabled()) {
            if (log.isInfoEnabled()){
                log.info("Constructing TcalBuilder");
            }
            tcalBuilderMonitor = new SecBuilderMonitor("TcalBuilder");
            tcalBufferCache = new ByteBufferCache(granularity,
                    maxCacheBytes,
                    maxAcquireBytes,
                    "TcalBuilder");
            tcalDispatcher = new FileDispatcher("tcal", tcalBufferCache);
            addCache(DAQConnector.TYPE_TCAL_DATA, tcalBufferCache);
            tcalFactory = new MasterPayloadFactory(tcalBufferCache);
            tcalSplicedAnalysis = new SBSplicedAnalysis(tcalFactory,
                    tcalDispatcher,
                    tcalBuilderMonitor);
            tcalSplicer = new SplicerImpl(tcalSplicedAnalysis);
            tcalSplicedAnalysis.setSplicer(tcalSplicer);
            tcalInputEngine = new SpliceablePayloadInputEngine(COMP_NAME,
                    COMP_ID, "stringHubTcalInput", tcalSplicer, tcalFactory);
            tcalBuilderMonitor.setPayloadInputEngine(tcalInputEngine);
            addEngine(DAQConnector.TYPE_TCAL_DATA, tcalInputEngine);
        }

        // init snBuilder
        if (compConfig.isSnEnabled()) {
            if (log.isInfoEnabled()){
                log.info("Constructing SNBuilder");
            }
            snBuilderMonitor = new SecBuilderMonitor("SNBuilder");
            snBufferCache = new ByteBufferCache(granularity,
                    maxCacheBytes,
                    maxAcquireBytes,
                    "SNBuilder");
            snDispatcher = new FileDispatcher("sn", snBufferCache);
            addCache(DAQConnector.TYPE_SN_DATA, snBufferCache);
            snFactory = new MasterPayloadFactory(snBufferCache);
            snSplicedAnalysis = new SBSplicedAnalysis(snFactory,
                    snDispatcher,
                    snBuilderMonitor);
            snSplicer = new SplicerImpl(snSplicedAnalysis);
            snSplicedAnalysis.setSplicer(snSplicer);
            snInputEngine = new SpliceablePayloadInputEngine(COMP_NAME,
                    COMP_ID, "stringHubSnInput", snSplicer, snFactory);
            snBuilderMonitor.setPayloadInputEngine(snInputEngine);
            addEngine(DAQConnector.TYPE_SN_DATA, snInputEngine);
        }

        // init moniBuilder classes
        if (compConfig.isMoniEnabled()) {
            if (log.isInfoEnabled()){
                log.info("Constructing MoniBuilder");
            }
            moniBuilderMonitor = new SecBuilderMonitor("MoniBuilder");
            moniBufferCache = new ByteBufferCache(granularity,
                    maxCacheBytes,
                    maxAcquireBytes,
                    "MonitorBuilder");
            moniDispatcher = new FileDispatcher("moni", moniBufferCache);
            addCache(DAQConnector.TYPE_MONI_DATA, moniBufferCache);
            moniFactory = new MasterPayloadFactory(moniBufferCache);
            moniSplicedAnalysis = new SBSplicedAnalysis(moniFactory,
                    moniDispatcher,
                    moniBuilderMonitor);
            moniSplicer = new SplicerImpl(moniSplicedAnalysis);
            moniSplicedAnalysis.setSplicer(moniSplicer);
            moniInputEngine = new SpliceablePayloadInputEngine(COMP_NAME,
                    COMP_ID, "stringHubMoniInput", moniSplicer, moniFactory);
            moniBuilderMonitor.setPayloadInputEngine(moniInputEngine);
            addEngine(DAQConnector.TYPE_MONI_DATA, moniInputEngine);
        }
    }

    public void setRunNumber(int runNumber) {
        if (log.isInfoEnabled()){
            log.info("Setting runNumber = " + runNumber);
        }
        if (compConfig.isTcalEnabled()) {
            tcalSplicedAnalysis.setRunNumber(runNumber);
        }
        if (compConfig.isSnEnabled()) {
            snSplicedAnalysis.setRunNumber(runNumber);
        }
        if (compConfig.isMoniEnabled()) {
            moniSplicedAnalysis.setRunNumber(runNumber);
        }
    }

    /**
     * Run a DAQ component server.
     *
     * @param args command-line arguments
     *
     * @throws icecube.daq.juggler.component.DAQCompException if there is a problem
     */
    public static void main(String[] args) throws DAQCompException {
        new DAQCompServer(new SBComponent(new SecBuilderCompConfig()), args);
    }
}