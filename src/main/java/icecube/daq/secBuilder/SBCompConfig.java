/**
 * Class: SBCompConfig
 *
 * Date: Nov 29, 2006 8:49:05 AM
 *
 * (c) 2004 IceCube collaboration
 */
package icecube.daq.secBuilder;

import icecube.daq.juggler.component.DAQCompConfig;

/**
 * This interface provides the config information for
 * secondary builders.
 */
public interface SBCompConfig extends DAQCompConfig {
 
    public boolean isTcalEnabled();

    public boolean isSnEnabled();

    public boolean isMoniEnabled();

    public boolean isMonitoring();
}
