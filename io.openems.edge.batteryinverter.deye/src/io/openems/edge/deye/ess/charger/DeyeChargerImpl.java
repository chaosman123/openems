package io.openems.edge.deye.ess.charger;

import io.openems.common.channel.AccessMode;
import io.openems.common.exceptions.OpenemsError;
import io.openems.common.exceptions.OpenemsException;
import io.openems.edge.bridge.modbus.api.AbstractOpenemsModbusComponent;
import io.openems.edge.bridge.modbus.api.BridgeModbus;
import io.openems.edge.bridge.modbus.api.ModbusComponent;
import io.openems.edge.bridge.modbus.api.ModbusProtocol;
import io.openems.edge.bridge.modbus.api.element.UnsignedWordElement;
import io.openems.edge.bridge.modbus.api.element.SignedWordElement;
import io.openems.edge.bridge.modbus.api.task.FC3ReadRegistersTask;
import io.openems.edge.common.component.OpenemsComponent;
import io.openems.edge.common.event.EdgeEventConstants;
import io.openems.edge.common.modbusslave.ModbusSlave;
import io.openems.edge.common.modbusslave.ModbusSlaveTable;
import io.openems.edge.common.taskmanager.Priority;
//import io.openems.edge.meter.api.ElectricityMeter;
//import io.openems.edge.meter.api.MeterType;
import io.openems.edge.ess.dccharger.api.EssDcCharger;
//import io.openems.edge.pvinverter.api.ManagedSymmetricPvInverter;
import io.openems.edge.timedata.api.Timedata;
import io.openems.edge.timedata.api.TimedataProvider;
import io.openems.edge.timedata.api.utils.CalculateEnergyFromPower;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.osgi.service.event.propertytypes.EventTopics;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;

@Designate(ocd = Config.class, factory = true)
@Component(//
                name = "Deye.Charger", //
                immediate = true, //
                configurationPolicy = ConfigurationPolicy.REQUIRE //
)
@EventTopics({ //
                EdgeEventConstants.TOPIC_CYCLE_BEFORE_PROCESS_IMAGE, //
		EdgeEventConstants.TOPIC_CYCLE_AFTER_PROCESS_IMAGE //
})

public class DeyeChargerImpl extends AbstractOpenemsModbusComponent
		implements DeyeCharger, EssDcCharger, ModbusComponent, OpenemsComponent, EventHandler, ModbusSlave, TimedataProvider {

	private final CalculateEnergyFromPower calculateActualEnergy = new CalculateEnergyFromPower(this, EssDcCharger.ChannelId.ACTUAL_ENERGY);

        @Reference
        private ConfigurationAdmin cm;

	@Override
        @Reference(policy = ReferencePolicy.STATIC, policyOption = ReferencePolicyOption.GREEDY, cardinality = ReferenceCardinality.MANDATORY)
	protected void setModbus(BridgeModbus modbus) {
		System.out.println("*** CHAOSMAN G ***");
		super.setModbus(modbus);
	}

        @Reference(policy = ReferencePolicy.DYNAMIC, policyOption = ReferencePolicyOption.GREEDY, cardinality = ReferenceCardinality.OPTIONAL)
        private volatile Timedata timedata = null;

	protected Config config;

        public DeyeChargerImpl() throws OpenemsException {
                super(//

                                OpenemsComponent.ChannelId.values(), //
                                ModbusComponent.ChannelId.values(), //
				EssDcCharger.ChannelId.values(), //
				DeyeCharger.ChannelId.values() //

		);
                System.out.println("*** CHAOSMAN A ***");
                DeyeCharger.calculateSumActivePowerFromPhases(this);
        }

        @Activate
        private void activate(ComponentContext context, Config config) throws OpenemsException {
		System.out.println("*** CHAOSMAN 1 ***");
                if (super.activate(context, config.id(), config.alias(), config.enabled(), config.modbusUnitId(), this.cm, "Modbus", config.modbus_id())) {
                        return;
                }
                this.config = config;
                this._setMaxActualPower(config.maxActualPower());

                // Stop if component is disabled
                if (!config.enabled()) {
                        return;
                }

        }

        @Deactivate
        @Override
        protected void deactivate() {
                super.deactivate();
        }

	@Override
	protected ModbusProtocol defineModbusProtocol() {
		System.out.println("*** CHAOSMAN B ***");
		return new ModbusProtocol(this, //
                                new FC3ReadRegistersTask(590, Priority.HIGH,
                                                m(DeyeCharger.ChannelId.ACTIVE_POWER, new SignedWordElement(619))),

				new FC3ReadRegistersTask(672, Priority.LOW, //
						m(DeyeCharger.ChannelId.ACTIVE_POWER_STRING_1, new UnsignedWordElement(672)),
						m(DeyeCharger.ChannelId.ACTIVE_POWER_STRING_2, new UnsignedWordElement(673)),
						m(DeyeCharger.ChannelId.ACTIVE_POWER_STRING_3, new UnsignedWordElement(674)),
						m(DeyeCharger.ChannelId.ACTIVE_POWER_STRING_4, new UnsignedWordElement(675))),
				new FC3ReadRegistersTask(667, Priority.LOW, //
								m(DeyeCharger.ChannelId.ACTIVE_POWER_GENERATOR, new UnsignedWordElement(667))));
	}

	@Override
	public void handleEvent(Event event) {
		System.out.println("*** CHAOSMAN 2 ***");
		switch (event.getTopic()) {
		//case EdgeEventConstants.TOPIC_CYCLE_AFTER_PROCESS_IMAGE:
		case EdgeEventConstants.TOPIC_CYCLE_BEFORE_PROCESS_IMAGE:
			Integer actualPower = this.getActualPower().get();
			if (actualPower == null)
				this._setActualPower(0);
			this.calculateActualEnergy.update(actualPower);
			System.out.println("CHAOSMAN getActualPower: " + actualPower);
			break;
		}
	}

	@Override
	public String debugLog() {
		return "L:" + this.getActualPower().asString();
	}

	@Override
	public ModbusSlaveTable getModbusSlaveTable(AccessMode accessMode) {
		System.out.println("*** CHAOSMAN C ***");
		return new ModbusSlaveTable(//
				OpenemsComponent.getModbusSlaveNatureTable(accessMode), //
				EssDcCharger.getModbusSlaveNatureTable(accessMode));
	}

        @Override
        public Timedata getTimedata() {
                return this.timedata;
        }

}
