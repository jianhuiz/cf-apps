#!/bin/bash
#name:create_trust.sh

if [ $# -ne 3 -a $# -ne 4 ]; then
    echo "Usage:"  
    echo "$0 user password hostsFile [logfile]"  
    exit 1  
fi

DEST_USER=$1
PASSWORD=$2 
HOSTS_FILE=$3
TMP_DIR="${HOME}/tmp_create_trust"
if [ -z $4 ]; then
    tmplogfile=${TMP_DIR}/create_ssh_trust.log
else
    tmplogfile=$4
fi

SSH_DIR="${HOME}/.ssh"
authfile="${HOME}/.ssh/authorized_keys"
knownhost="${HOME}/.ssh/known_hosts"
pre_trust="TRUST"
authfile_bak="${TMP_DIR}/authorized_keys_bak"
knownhost_bak="${TMP_DIR}/known_hosts"

AuSCP()
{
  expect<<EOF
  spawn /usr/bin/scp -p "$1" "$2"
  set timeout 300
  while {1} {
    expect {
      timeout break
      "continue" {
        send "yes\n"
        while {1} {
          expect {
            timeout {
            send \003
            exit 1
            }
            "assword" break
            }
        }
        send "$3\n"
        while {1} {
          expect {
            timeout {
            send \003
            exit 1
            }
            "100%" {
                  set timeout 3
                  expect eof
                  exit 0
                }
                  "assword" {
                  send \003
                  expect eof
                  exit 1
                  }
          }
        }
        break
      }
      "assword" {
        send "$3\n"
        while {1} {
          expect {
            timeout {
            send \003
            exit 1
            }
            "100%" {
                  expect eof
                  exit 0
                }
                "assword" {
                  send \003
                  expect eof
                  exit 1
                }
          }
        }
        break
      }      
      "100%" {
        set timeout 3
        expect eof
        exit 0
      }
    }
  }
  expect eof
EOF
}

AuSSH()
{
  expect<<EOF
  spawn ssh "$1" "$2"
  set timeout 300
  while {1} {
    expect {
      timeout break
      "continue" {
        send "yes\n"
        while {1} {
          expect {
            timeout {
            send \003
            exit 1
            }
            "assword" break
            }
        }
        send "$3\n"
        expect {
            timeout break
            "assword" {
            send \003
            exit 1
            }
        }
        break
      }
      "assword" {
        send "$3\n"
        expect {
            timeout break
            "assword" {
              send \003
              exit 1
            }
        }
        break
      }
    }
  }
  exit 0
EOF
}

die()
{
    echo "$@" >> ${tmplogfile}
    echo "$@"
    exit 1
}

checkPara()
{
  local -i pingtimes=3
  local -i pingpackage=0
  for ip in $(cat $HOSTS_FILE)
  do 
      pingpackage=0
      echo "begin ping ${ip} ..." 
      pingpackage=$(ping ${ip} -i 1 -c ${pingtimes} | grep ttl | wc -l )
      [ ${pingpackage} -ne ${pingtimes} ] && echo "ping ${ip} not well." && return 1
      echo "ping ${ip} returns well." 
  done
  return 0
}

#clean tmp dir
rm -rf ${TMP_DIR}

#check parameter
checkPara || exit 1

#clean files
rm -rf ${SSH_DIR}/* 

#generate ssh key
mkdir -p ${TMP_DIR} -m 755
echo "begin generate ssh key on all hosts ..." | tee -a ${tmplogfile}
for ip in $(cat $HOSTS_FILE)
do
    AuSSH ${DEST_USER}@${ip} "rm -rf ${SSH_DIR}" ${PASSWORD} >> ${tmplogfile} 2>&1
	if [ $? -ne 0 ]; then
		die "clean ssh path on ${ip} failed." 
	fi
    AuSSH ${DEST_USER}@${ip} "ssh-keygen -t rsa -q -f ${SSH_DIR}/id_rsa -P ''" ${PASSWORD} >> ${tmplogfile} 2>&1
	if [ $? -ne 0 ]; then
		die "generate new ssh key on ${ip} failed." 
	fi
    AuSCP ${DEST_USER}@${ip}:${SSH_DIR}/id_rsa.pub ${TMP_DIR}/${pre_trust}_${ip}.pub ${PASSWORD} >> ${tmplogfile} 2>&1
	if [ $? -ne 0 ]; then
		die "copy public key of ${ip} to local host failed." 
	fi
done
echo "generate ssh key on all hosts finished." | tee -a ${tmplogfile}

#integrate ssh key
echo "begin integrate ssh key ..." | tee -a ${tmplogfile}
cat ${TMP_DIR}/${pre_trust}_*.pub > ${authfile_bak}
if [ $? -ne 0 ]; then
	die "integrate public key failed." 
fi
echo "integrate ssh key finished." | tee -a ${tmplogfile}
#prepare knowhosts file
echo "begin prepare knowhosts file ..." | tee -a ${tmplogfile}
#should remove knows_hosts file
if [ -f ${knownhost} ];then rm ${knownhost};fi
for ip in $(cat $HOSTS_FILE)
do
    if [ "x$ip" != "x" ]; then
        val=`ssh-keygen -F $ip 2>/dev/null` 
        if [ "x$val" == "x" ]; then
            val=`ssh-keyscan $ip 2>/dev/null`
            if [ "x$val" == "x" ]; then
                echo "ssh-keyscan $ip failed!" | tee -a ${tmplogfile}
            else  
                echo $val >> ${knownhost_bak} | tee -a ${tmplogfile}
        fi 
        fi
    fi
done
echo "prepare knowhosts file finished." | tee -a ${tmplogfile}

#distribute all files to other hosts
echo "begin distribute files to other hosts ..." | tee -a ${tmplogfile}
for ip in $(cat $HOSTS_FILE)
do
  AuSCP ${authfile_bak} ${DEST_USER}@${ip}:${authfile} ${PASSWORD} >> ${tmplogfile} 2>&1 
  if [ $? -ne 0 ]; then
	die "distribute authfile to ${ip} failed." 
  fi
  AuSCP ${knownhost_bak} ${DEST_USER}@${ip}:${knownhost} ${PASSWORD} >> ${tmplogfile} 2>&1
  if [ $? -ne 0 ]; then
	die "distribute knowhost to ${ip} failed." 
  fi
done
echo "distribute files to other hosts finished." | tee -a ${tmplogfile}

#clean tmp dir and log create trust succeed
rm -rf ${TMP_DIR}
echo "make hosts trust successfully."

exit 0
