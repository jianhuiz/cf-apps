#!/bin/bash

action=""
current_dir=$(cd "$(dirname "$0")"; pwd)
gauss_upgrade="${current_dir}/script/GaussUpgrade.py"
xml_file="/opt/huawei/wisequery/clusterconfig.xml"
upd_file="/tmp/pgsql/upgrade_step.dat"
#------------------------------------------------------------------------------
# Main
#------------------------------------------------------------------------------

function scp_db()
{
    local rc=0
    local nodenames=`readcluster cluster nodeNames 2>nul`
    if [ x"${nodenames}" = x"" ]; then
        echo "read node names failed."
        exit 1
    fi

    nodenames=${nodenames//,/ }
    if [ x"${nodenames}" = x"" ]; then
        echo "there is no node."
        exit 1
    fi

    local_node=`hostname`

    for node in ${nodenames[@]}
    do
        if [ x"$node" = x"$local_node" ]; then
            echo "$node is local node, do not copy file"
        else
            ssh $node "mkdir -p /opt/huawei/snas/upd_pkg/OceanStor_9000"
            rc=$?
            if [ $rc -ne 0 ]; then
                echo "mkdir in $node failed."
                exit 1
            fi

            scp -r /opt/huawei/snas/upd_pkg/OceanStor_9000/gaussdb root@${node}:/opt/huawei/snas/upd_pkg/OceanStor_9000/gaussdb
            rc=$?
            if [ $rc -ne 0 ]; then
                echo "scp to $node failed."
                exit 1
            fi
        fi
    done
}

if [ -n "$1" ];then
    action="$1"
fi

if [ x"${action}" = x"precheck" ];then
    python ${gauss_upgrade} -t healthcheck -o before -X ${xml_file}
elif [ x"${action}" = x"online" ];then
    scp_db
    python ${gauss_upgrade} -t onlineupgrade -X ${xml_file}
elif [ x"${action}" = x"offline" ];then
    python ${gauss_upgrade} -t offlineupgrade -X ${xml_file}
elif [ x"${action}" = x"postcheck" ];then
    python ${gauss_upgrade} -t healthcheck -o after -X ${xml_file}
elif [ x"${action}" = x"rollback" ];then
    if [ -f ${upd_file} ];then
        rollbacktype="onlinerollback"
        nodeName=`cat ${upd_file} | awk -F ":" '{print $1}'`
        if [ x"$nodeName" = x"" ];then
            rollbacktype="offlinerollback"
        fi
        python ${gauss_upgrade} -t ${rollbacktype} -X ${xml_file}
    else
        echo "The cluster is normal now.No need to rollback."
        exit 0
    fi
else
    echo "Unknown parameter :${action}."
    exit 1
fi
