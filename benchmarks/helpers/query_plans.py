import glob, os
from subprocess import check_call
from veung import veung


def save_plan_trees_from_dir_as_ps(file_infix, plan_dir):
    """
        infixes:
        "*.geqo.out_plan*"
        "*.saio.out_plan*"
    """
    os.chdir(plan_dir)
    for in_fname in glob.glob(file_infix):
        print in_fname
        save_plan_tree_as_ps(in_fname, in_fname+".ps")


def save_plan_tree_as_ps(in_fname, out_fname):
    dot_fname = out_fname+".dot"
    save_plan_tree_as_dot_file(in_fname, dot_fname)
    #dot file.dot -Tpng -o image.png

    check_call(['dot','-Tps', dot_fname,'-o', out_fname])



def save_plan_tree_as_dot_file(in_fname, out_fname):
    veung.save_to_file(in_fname, out_fname)


def depth(x):
    "depth of a dict"
    if type(x) is dict and x:
        return 1 + max(depth(x[a]) for a in x)
    if type(x) is list and x:
        return 1 + max(depth(a) for a in x)
    return 0


def depth_of_json_from_file(fname):
    with open(fname) as f:
        plan_json = json.loads(fname)
    return depth(plan_json)