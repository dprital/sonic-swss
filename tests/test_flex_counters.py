import time
import pytest

# Counter keys on ConfigDB
PORT_KEY                  =   "PORT"
QUEUE_KEY                 =   "QUEUE"
RIF_KEY                   =   "RIF"
BUFFER_POOL_WATERMARK_KEY =   "BUFFER_POOL_WATERMARK"
PORT_BUFFER_DROP_KEY      =   "PORT_BUFFER_DROP"
PG_WATERMARK_KEY          =   "PG_WATERMARK"

# Counter stats on FlexCountersDB
PORT_STAT                  =   "PORT_STAT_COUNTER"
QUEUE_STAT                 =   "QUEUE_STAT_COUNTER"
RIF_STAT                   =   "RIF_STAT_COUNTER"
BUFFER_POOL_WATERMARK_STAT =   "BUFFER_POOL_WATERMARK_STAT_COUNTER"
PORT_BUFFER_DROP_STAT      =   "PORT_BUFFER_DROP_STAT"
PG_WATERMARK_STAT          =   "PG_WATERMARK_STAT_COUNTER"

# Counter maps on CountersDB
PORT_MAP                  =   "COUNTERS_PORT_NAME_MAP"
QUEUE_MAP                 =   "COUNTERS_QUEUE_NAME_MAP"
RIF_MAP                   =   "COUNTERS_RIF_NAME_MAP"
BUFFER_POOL_WATERMARK_MAP =   "COUNTERS_BUFFER_POOL_NAME_MAP"
PORT_BUFFER_DROP_MAP      =   "COUNTERS_PORT_NAME_MAP"
PG_WATERMARK_MAP          =   "COUNTERS_PG_NAME_MAP"

NUMBER_OF_RETRIES         =   10
CPU_PORT_OID              = "0x0"

counter_type_dict = {"port_counter":[PORT_KEY, PORT_STAT, PORT_MAP],
                     "queue_counter":[QUEUE_KEY, QUEUE_STAT, QUEUE_MAP],
                     "rif_counter":[RIF_KEY, RIF_STAT, RIF_MAP],
                     "buffer_pool_watermark_counter":[BUFFER_POOL_WATERMARK_KEY, BUFFER_POOL_WATERMARK_STAT, BUFFER_POOL_WATERMARK_MAP],
                     "port_buffer_drop_counter":[PORT_BUFFER_DROP_KEY, PORT_BUFFER_DROP_STAT, PORT_BUFFER_DROP_MAP],
                     "pg_watermark_counter":[PG_WATERMARK_KEY, PG_WATERMARK_STAT, PG_WATERMARK_MAP]}

class TestFlexCounters(object):

    def setup_dbs(self, dvs):
        self.config_db = dvs.get_config_db()
        self.flex_db = dvs.get_flex_db()
        self.counters_db = dvs.get_counters_db()

    def wait_for_table(self, table):
        for retry in range(NUMBER_OF_RETRIES):
            counters_keys = self.counters_db.db_connection.hgetall(table)
            if len(counters_keys) > 0:
                return
            else:
                time.sleep(1)

        assert False, str(table) + " not created in Counters DB"

    def wait_for_id_list(self, stat, name, oid):
        for retry in range(NUMBER_OF_RETRIES):
            id_list = self.flex_db.db_connection.hgetall("FLEX_COUNTER_TABLE:" + stat + ":" + oid).items()
            if len(id_list) > 0:
                return
            else:
                time.sleep(1)

        assert False, "No ID list for counter " + str(name)

    def verify_no_flex_counters_tables(self, counter_stat):
        counters_stat_keys = self.flex_db.get_keys("FLEX_COUNTER_TABLE:" + counter_stat)
        assert len(counters_stat_keys) == 0, "FLEX_COUNTER_TABLE:" + str(counter_stat) + " tables exist before enabling the flex counter group"

    def verify_flex_counters_populated(self, map, stat):
        counters_keys = self.counters_db.db_connection.hgetall(map)
        for counter_entry in counters_keys.items():
            name = counter_entry[0]
            oid = counter_entry[1]
            self.wait_for_id_list(stat, name, oid)

    def verify_only_phy_ports_created(self):
        port_counters_keys = self.counters_db.db_connection.hgetall(PORT_MAP)
        port_counters_stat_keys = self.flex_db.get_keys("FLEX_COUNTER_TABLE:" + PORT_STAT)
        for port_stat in port_counters_stat_keys:
            assert port_stat in dict(port_counters_keys.items()).values(), "Non PHY port created on PORT_STAT_COUNTER group: {}".format(port_stat)

    def enable_flex_counter_group(self, group, map):
        group_stats_entry = {"FLEX_COUNTER_STATUS": "enable"}
        self.config_db.create_entry("FLEX_COUNTER_TABLE", group, group_stats_entry)
        self.wait_for_table(map)

    @pytest.mark.parametrize("counter_type", counter_type_dict.keys())
    def test_flex_counters(self, dvs, counter_type):
        """
        The test will check there are no flex counters tables on FlexCounter DB when the counters are disabled.
        After enabling each counter group, the test will check the flow of creating flex counters tables on FlexCounter DB.
        For some counter types the MAPS on COUNTERS DB will be created as well after enabling the counter group, this will be also verified on this test.
        """
        self.setup_dbs(dvs)
        counter_key = counter_type_dict[counter_type][0]
        counter_stat = counter_type_dict[counter_type][1]
        counter_map = counter_type_dict[counter_type][2]

        self.verify_no_flex_counters_tables(counter_stat)

        if counter_type == "rif_counter":
            self.config_db.db_connection.hset('INTERFACE|Ethernet0', "NULL", "NULL")
            self.config_db.db_connection.hset('INTERFACE|Ethernet0|192.168.0.1/24', "NULL", "NULL")

        self.enable_flex_counter_group(counter_key, counter_map)
        self.verify_flex_counters_populated(counter_map, counter_stat)

        if counter_type == "port_counter":
            self.verify_only_phy_ports_created()

        if counter_type == "rif_counter":
            self.config_db.db_connection.hdel('INTERFACE|Ethernet0|192.168.0.1/24', "NULL")

    def remove_port(self, config_db, port):
        config_db.delete_entry("CABLE_LENGTH|AZURE", "")
        ethernet0_bufferpg_keys = config_db.get_keys("BUFFER_PG|%s"%port)
        for key in ethernet0_bufferpg_keys:
            config_db.delete_entry("BUFFER_PG|Ethernet0|%s"%key, "")
        ethernet0_bufferqueue_keys = config_db.get_keys("BUFFER_QUEUE|%s"%port)
        for key in ethernet0_bufferqueue_keys:
            config_db.delete_entry("BUFFER_QUEUE|Ethernet0|%s"%key, "")
        config_db.delete_entry("BREAKOUT_CFG|%s"%port, "")
        config_db.delete_entry("INTERFACE|%s"%port, "")
        config_db.delete_entry("PORT", port)
               
    def test_add_remove_ports(self, dvs):
        self.setup_dbs(dvs)
        
        # set flex counter
        counter_key = counter_type_dict['queue_counter'][0]
        counter_stat = counter_type_dict['queue_counter'][1]
        counter_map = counter_type_dict['queue_counter'][2]
        self.enable_flex_counter_group(counter_key, counter_map)
        time.sleep(3)
        
        # receive Ethernet0 info
        fvs = self.config_db.get_entry("PORT", "Ethernet0")
        assert len(fvs) > 0
        
        # save all the oids of the pg drop counters            
        oid_list = []
        counters_queue_map = self.counters_db.get_entry("COUNTERS_QUEUE_NAME_MAP","")
        
        for i in range(0,100):
            if 'Ethernet0:%d'%i in counters_queue_map:
                oid_list.append(counters_queue_map['Ethernet0:%d'%i])
            else:
                break

        # verify that counters exists on flex counter
        for oid in oid_list:   
            fields = self.flex_db.get_entry("FLEX_COUNTER_TABLE", counter_stat + ":%s"%oid)
            assert len(fields) == 1

        # remove port Ethernet0
        self.remove_port(self.config_db, "Ethernet0")
        time.sleep(3)
        
        
        # verify counters were removed from flex counter table
        for oid in oid_list:
            fields = self.flex_db.get_entry("FLEX_COUNTER_TABLE", counter_stat + ":%s"%oid)
            assert len(fields) == 0
        
        # verify that Ethernet0 counter maps were removed
        oid_list = []
        counters_queue_map = self.counters_db.get_entry("COUNTERS_QUEUE_NAME_MAP","")
        
        for i in range(0,100):
            if 'Ethernet0:%d'%i in counters_queue_map:
                oid_list.append(counters_queue_map['Ethernet0:%d'%i])
            else:
                break
        assert oid_list == []
        
            
        # add port Ethernet 0 
        self.config_db.create_entry("PORT","Ethernet0",fvs)
        time.sleep(3)
        
        # verify counter was added
        oid_list = []
        counters_queue_map = self.counters_db.get_entry("COUNTERS_QUEUE_NAME_MAP","")
        
        for i in range(0,100):
            if 'Ethernet0:%d'%i in counters_queue_map:
                oid_list.append(counters_queue_map['Ethernet0:%d'%i])
            else:
                break

        # verify that counters exists on flex counter
        for oid in oid_list:   
            fields = self.flex_db.get_entry("FLEX_COUNTER_TABLE", counter_stat + ":%s"%oid)
            assert len(fields) == 1
        
