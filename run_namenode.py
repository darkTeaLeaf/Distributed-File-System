from namenode.namenode import Namenode
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--ip', type=str, required=True,
                        help='IP of the Namenode')
    parser.add_argument('--lock_duration', type=int, required=True,
                        help='Duration of the lock for read or written files')
    parser.add_argument('--update_time', type=int, required=True,
                        help='Period of checking locks in files')
    parser.add_argument('--num_replicas', type=int, required=True,
                        help='Number of the replicas in the file system')
    args = parser.parse_args()

    node = Namenode(args.ip, 80, args.num_replicas, lock_duration=args.lock_duration, update_time=args.update_time,
                    user="Namenode", passwd="1234576890")

    node.start()
